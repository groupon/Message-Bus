# Copyright (c) 2012, Groupon, Inc.
# All rights reserved.
# 
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
# 
# Redistributions of source code must retain the above copyright notice,
# this list of conditions and the following disclaimer.
# 
# Redistributions in binary form must reproduce the above copyright
# notice, this list of conditions and the following disclaimer in the
# documentation and/or other materials provided with the distribution.
# 
# Neither the name of GROUPON nor the names of its contributors may be
# used to endorse or promote products derived from this software without
# specific prior written permission.
# 
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
# IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
# TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
# PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
# HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
# SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
# TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
# PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
# LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
# NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

require 'thread'
require 'net/http'
require "messagebus/dottable_hash"

module Messagebus
  # Consumer client class. Provides a single access thread for all messagebus servers.
  # Takes in a list of messagebus servers to receive from, open connections to
  # all servers, does round robin receives across all servers.
  #
  #   parameters:
  #   dest      (String, required value, name of the queue/topic)
  #   host_params      (list<string>, required value,  eg. '[localhost:61613]')
  #   options : A hash map for optional values.
  #     user     (String,  default : '')
  #     passwd  (String,  default : '')
  #     ack_type      (String, required value: Messagebus::ACK_TYPE_AUTO_CLIENT OR Messagebus::ACK_TYPE_CLIENT)
  #                   autoClient: module acks internally automatically for each receive.
  #                   client: User should explicit ack *each* message.
  #     conn_lifetime_sec      (Int, default:300 secs)
  #     subscription_id      (String, required for topic, Each subscription is identified by a unique Id,
  #                                  for a topic different subscriptions means each subscription gets copy of
  #                                  message each, same subscription_id across multiple Consumers means load-balancing
  #                                  messages for that subscription.)
  #     enable_dynamic_serverlist_fetch:  (Boolean, Enable the consumer to fetch the list of brokers actively default: true)
  #     dynamic_serverlist_fetch_url_override      (String, The override url to fetch the list of consumers dynamically.)
  #     dynamic_fetch_timeout_ms  (Integer, milliseconds to wait for http response of dynamic serverlist fetch)
  #     receipt_wait_timeout_ms (Int, optoional value, default: 5 seconds)

  class Consumer < Connection
    attr_accessor :received_messages, :servers_running, :state

    def self.start(host_params, options={})
      consumer = new(host_params, options)
      consumer.start
      if block_given?
        begin
          yield consumer
        ensure
          consumer.stop
        end
      end
      consumer
    end

    def initialize(host_params, options = {})
      options = DottableHash.new({:ack_type => Messagebus::ACK_TYPE_CLIENT, :enable_dynamic_serverlist_fetch => true }).merge(options)
      options.merge!(options.cluster_defaults) if options.cluster_defaults

      super(host_params, options)

      validate_destination_config(@options.destination_name, true, options)
      validate_connection_config(@host_params, options)

      @received_messages = Queue.new
      @servers_running = {}
      @logger = Logger.new(options[:log_file]) if options[:log_file]
    end

    def logger
      @logger ||= Client.logger
    end

    # Start the consumers and all connections.
    # Optionally takes a block to which it yields self. When the block is
    # passed, it will auto close the connections after the block finishes.
    def start
      @state = STARTED
      logger.info("Starting consumers with host_params:#{@host_params.inspect} for destination:#{@options.destination_name}")
      start_servers(@host_params, true)
      refresh_servers
    end

    # Close the consumers and all connections
    def stop
      @state = STOPPED
      logger.info("Stopping consumers for running servers:#{@servers_running.keys.inspect}")

      stop_servers(@servers_running.keys)
    end

    ##
    # This is used to insert an unblock message into the consumer. A use case
    # is when you're using a blocking receive, and you want to unblock a
    # separate thread or tell a consumer to unblock from a signal handler.
    # See also Messagebus::Swarm::Drone#stop
    #
    # http://en.wikipedia.org/wiki/Sentinel_value
    def insert_sentinel_value(final_message=nil)
      # push a message onto our consumer so that if we're currently blocking on waiting for a message
      # we'll see this and do no further processing
      @received_messages.push({:stop_processing_sentinel => true, :msg => final_message})
    end

    # Blocking receive: block till a value is available.
    # Returns the message(Messagebus::Message) received or block indefinately.
    def receive
      return receive_internal(non_blocking=false)
    end

    # Blocking receive with timeout: block till a value is available for passed timeout.
    # Returns the message(Messagebus::Message) received or raise MessageReceiveTimeout("timeout")
    def receive_timeout(timeout_ms=1000)
      do_with_timeout(timeout_ms) {
        if @received_messages.empty?
          sleep 0.01
        else
          return receive_internal(non_blocking=true)
        end
      }

      raise MessageReceiveTimeout, "receive timeout(" + timeout_ms.to_s +  ") while waiting for message to arrive."
    end

    # Non-Blocking receive.
    # Returns the message(Messagebus::Message) received or nil immediately.
    def receive_immediate()
      if not @received_messages.empty?
        return receive_internal(non_blocking=true)
      else
        return nil
      end
    end

    # Send consumer credit back for last received message.
    def credit()
      if not @last_received.nil?
        begin
          logger.info("Sending consumer credit for message with id:#{@last_received[:decoded_msg].message_id}")

          @servers_running[@last_received[:host_param]].credit(@last_received[:msg])
        rescue NameError => e
          logger.error("Failed to credit message. Was the connection removed?. #{e.message} #{e.backtrace.join("|")}")
        end
      end
    end

    # Ack the last received message.
    # Message broker will keep resending messages (after retry_wait and upto retry_max_times) till
    # it sees an ack for the message.
    def ack(safe_mode = false)
      if not @last_received.nil?
        begin
          logger.info("Sending ack() for message with id:#{@last_received[:decoded_msg].message_id}")
          if not safe_mode
            begin
              @servers_running[@last_received[:host_param]].acknowledge(@last_received[:msg])
              @last_received = nil
              return true
            
            rescue => e
              logger.error("Failed to ack message. Was the connection removed? #{e.message} #{e.backtrace.join("|")}")
            end
          else
            receipt_received = false
            errors_received = nil
            @servers_running[@last_received[:host_param]].acknowledge(@last_received[:msg]) do |msg|
              if msg.command == 'ERROR'
                errors_received = msg
                raise "Failed to ack message with Error: #{msg.body.to_s} #{caller}"
              else
                receipt_received = true
                @last_received = nil
              end
            end

            # wait for receipt up to given timeout.
            do_with_timeout(@options.receipt_wait_timeout_ms) do
              if errors_received
                raise "Failed to ack message in safe mode with Error: " + errors_received.body.to_s
              end

              if not receipt_received
                sleep 0.005
              else
                return true
              end
            end
          end
        end
      end
    end

    def nack
      if not @last_received.nil?
        begin
          logger.info("Sending nack() for message with id:#{@last_received[:decoded_msg].message_id}")

          @servers_running[@last_received[:host_param]].nack(@last_received[:msg])
          @last_received = nil
        rescue => e
          logger.error("Failed to nack message. Was the connection removed? #{e.message} #{e.backtrace.join("|")}")
        end
      end
    end

    def keepalive
      @servers_running.each do |host_param, client|
        begin
          client.keepalive()
          @last_received = nil
        rescue => e
          logger.error("Failed to send keepalive to #{host_param}")
        end
      end
    end


    def fetch_serverlist
      if @options.dynamic_serverlist_fetch_url_override
        dynamic_serverlist_fetch_url = @options.dynamic_serverlist_fetch_url_override
      else
        dynamic_serverlist_fetch_url = get_dynamic_fetch_url(@host_params)
      end

      logger.info("trying to fetch dynamic url #{dynamic_serverlist_fetch_url}")
      begin
        data = fetch_uri(dynamic_serverlist_fetch_url)
        data = data.gsub(' ', '')
        serverlist = data.split(',')
        serverlist.each do |server|
          if SERVER_REGEX.match(server).nil?
            raise "bad data returned from dynamic url: #{data}"
          end
        end
        return serverlist

      rescue => e
        logger.error("Failed to fetch server list from url:#{dynamic_serverlist_fetch_url} with exception: #{e.message}, #{e.backtrace.join("|")}")
        return nil
      end
    end

    def get_dynamic_fetch_url(host_params)
      case host_params
      when Array
        host_param = host_params[rand(host_params.length)]
      when String
        host_param = host_params
      end

      host, port = host_param.split(':')
      return 'http://' + host + ':8081/jmx?command=get_attribute&args=org.hornetq%3Amodule%3DCore%2Ctype%3DServer%20ListOfBrokers'
    end

    def delete_subscription()
      host_params = @servers_running.keys
      if not host_params.nil?
        host_params.each do |host_param|
          logger.info("Unsubscribing #{@options.destination_name} consumer client for #{host_param}")
          client = @servers_running[host_param]
          client.unsubscribe(@options.destination_name)
        end
      end
    end

    def refresh_servers
      logger.info("refreshing consumer threads.")
      @refresh_time = Time.new()
      hosts = @servers_running.keys
      if @options.enable_dynamic_serverlist_fetch
        # Fetch the server list from the dynamic server list fetch url.
        begin
          updated_server_list = fetch_serverlist
        rescue => e
          logger.error "Error in refresh server #{e} \n Stack Trace: #{e.backtrace.join("|")}"
        end

        if not updated_server_list.nil?
          hosts = updated_server_list
        end
      end

      logger.info("refreshing servers current_list:#{@servers_running.keys.inspect} new list:#{hosts.inspect}")

      servers_added = hosts - @servers_running.keys

      # start new servers
      if servers_added and not servers_added.empty?
        logger.info("Adding new servers in:#{servers_added.inspect}")
        if not servers_added.empty?()
          start_servers(servers_added)
        end
      end

    end

    private
    def fetch_uri(uri)
      uri = URI(uri)
      begin
        if uri and http = Net::HTTP.new(uri.host, uri.port)
          http.open_timeout = @options.dynamic_fetch_timeout_ms/1000.0
          http.read_timeout = @options.dynamic_fetch_timeout_ms/1000.0
          http.start {|http|
            response = http.request_get(uri.request_uri)
              if response.class == Net::HTTPOK
               logger.info("fetched_uri: #{response.body}")
               return response.body
              else
                logger.error("fetch_uri got bad response from server:#{response}")
                return nil
            end
          }
        end
      rescue => e
        logger.error("Failed to fetch dynamic server list with exception: #{e.message}, #{e.backtrace.join("|")}")
        return nil
      end
    end


    def subscribe_client(client)
      # Add the options for the client.
      # ack is 'client' for server in both 'client'/'autoClient' cases.
      options = {:ack => "client"}
      if not @options.subscription_id.nil?
        options["durable-subscriber-name"] = @options.subscription_id
        options["id"] = @options.subscription_id
        options["client-id"] = @options.subscription_id
      else
        # We need to set 'id' irrespective here.
        options[:id] = @options.destination_name
      end

      client.subscribe(@options.destination_name, options) do |msg|
        decoded_msg = nil
        if msg.command != 'ERROR'
          begin
            decoded_msg = Messagebus::Message.get_message_from_thrift_binary(msg.body)
            decoded_msg.message_properties = msg.headers
            @received_messages.push({:msg => msg, :host_param => client.host + ":" + client.port.to_s, :decoded_msg => decoded_msg})
          rescue => e
            logger.error("Failed to decode message:\n#{msg}\n#{e.message} #{e.backtrace.join("|")}")
          end
        else
          logger.info("ERROR frame received:\n#{msg}" )
        end

      end
    end

    def start_servers(host_params, fail_on_error = false)
      host_params = [host_params] if host_params.is_a?(String)

      if host_params
        host_params.each do |host_param|
          begin
            logger.info("Starting messagebus consumer client for #{host_param}")

            client = start_server(host_param, @options.user, @options.passwd, @options.subscription_id)
            subscribe_client(client)
            @servers_running[host_param] = client
          rescue => e
            logger.error("Failed to start server #{host_param} with exception: #{e.message}, #{e.backtrace.join("|")}")
            raise if fail_on_error
          end
        end
      end
    rescue
      stop
      raise
    end

    def stop_servers(host_params)
      if not host_params.nil?
        host_params.each do |host_param|
          host, port = host_param.split(':')
          if @servers_running.has_key?(host_param)
            begin
              logger.info("Stopping messagebus consumer client for #{host_param}")
              client = @servers_running[host_param]
              stop_server(client)
              @servers_running.delete(host_param)
            rescue => e
              logger.error("Failed to stop server #{host_param} with exception: #{e.message}, #{e.backtrace.join("|")}")
            end
          end
        end
      end
    end

    def receive_internal(non_blocking=false)
      current_time = Time.new();
      if current_time > @refresh_time + @options.conn_lifetime_sec
        refresh_servers
      end

      received_message = @received_messages.pop(non_blocking)
      if received_message[:stop_processing_sentinel]
        return received_message[:msg]
      end

      @last_received = received_message
      if !@last_received.nil?
        original_message = @last_received[:msg]
        if original_message.command == 'ERROR'
          logger.error("received error frame from server:\n#{original_message}")
          raise ErrorFrameReceived, "received error frame from server:\n#{original_message}"
        else
          message = @last_received[:decoded_msg]
          logger.info("received message with id:#{message.message_id}")

          # send back credits for this message.
          credit

          # send back the ack if user has choosen autoClient ack mode.
          if @options.ack_type == Messagebus::ACK_TYPE_AUTO_CLIENT
            ack
          end

          return message
        end
      end
    end

  end
end
