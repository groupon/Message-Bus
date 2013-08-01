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
  # Producr client class. Provides simple API to publish events. Refresh connections every connectionLifetime user can specify a load-balancer
  # and the producer will connect with different server each connectionLifetime interval effectively rotating publish load to all machines.
  #
  #   parameters:
  #   dest      (String, required value, name of the queue/topic)
  #   host_params      (list<string>, required value,  eg. '[localhost:61613]')
  #   options : A hash map for optional values.
  #     user     (String,  default : '')
  #     passwd  (String,  default : '')
  #     conn_lifetime_sec      (Int, default:300 secs)
  #     receipt_wait_timeout_ms (Int, optoional value, default: 5 seconds)
  #
  class Producer < Connection
    attr_accessor :state
    PUBLISH_HEADERS = { :suppress_content_length => true, :persistent => true }
    SCHEDULED_DELIVERY_TIME_MS_HEADER =  'scheduled_delivery_time'

    def initialize(host_params, options={})
      options = DottableHash.new(options)
      super(host_params, options)
      validate_connection_config(@host_params, options)
    end

    # Start the producer client
    def start
      @state = STARTED
      logger.info "Starting producer with host_params:#{@host_params}"
      @connection_start_time = Time.new
      @stomp = start_server(@host_params, @options.user, @options.passwd)
    rescue => e
      logger.error "Error occurred while starting a connection: #{e}\n #{e.backtrace.join("\n")}"
    end

    # Close the producer client
    def stop
      @state = STOPPED
      logger.info "Stopping producer with host_params:#{@host_params}"
      stop_server(@stomp)
    rescue => e
      logger.error "Error occurred while stopping a connection: #{e}\n #{e.backtrace.join("\n")}"
    end

    # This is implemented with a *args to workaround the historical api
    # requiring a dest_type parameter. That parameter has been removed, but the
    # api has been kept backwards compatible for now.
    #
    # Historical version
    #   def publish(dest, dest_type, message, connect_headers={}, safe=true)
    # For the current version see actual_publish.
    def publish(*args)
      if args.size > 2 && args[1].is_a?(String) && (args[1].downcase == 'topic' || args[1].downcase == 'queue')
        logger.warn "Passing dest_type to Producer#publish is deprecated (it isn't needed). Please update your usage."
        args.delete_at(1)
      end
      actual_publish(*args)
    end

    # This is the actual publish method. See publish for why this is designed this way.
    def actual_publish(dest, message, connect_headers={}, safe=true)
      if !started?
        logger.error "Cannot publish without first starting the producer. Current state is '#{@state}'"
        return
      end
      validate_destination_config(dest)
      publish_internal(dest, message, connect_headers, safe)
    rescue => e
      logger.error "Error occurred while publishing the message: #{e}\n #{e.backtrace.join("\n")}"
      return false
    end
    
    private

    def publish_internal(dest, message, connect_headers, safe_mode=false)
      if not message.is_a?(Messagebus::Message)
        raise "ERROR: message should be a Messagebus::Message type."
      end

      check_and_refresh_connection
      logger.debug "publishing message with message Id:#{message.message_id} safe_mode:#{safe_mode}"
      logger.debug { "message: #{message.inspect}" }

      connect_headers = connect_headers.merge(PUBLISH_HEADERS)
      if not safe_mode
        @stomp.publish(dest, message.to_thrift_binary, connect_headers)
        return true
      else
        receipt_received = false
        errors_received = nil
        @stomp.publish(dest, message.to_thrift_binary, connect_headers) do |msg|
          if msg.command == 'ERROR'
            errors_received = msg
            raise "Failed to publish the message while publishing to #{dest} to the server with Error: #{msg.body.to_s} #{caller}"
          else
            receipt_received = true
          end
        end

        # wait for receipt up to given timeout.
        do_with_timeout(@options.receipt_wait_timeout_ms) do
          if errors_received
            raise "Failed to publish the message while publishing to #{dest} to the server with Error:\n" + errors_received.body.to_s
          end

          if not receipt_received
            sleep 0.005
          else
            return true
          end
        end

        logger.error "Publish to #{dest} may have failed, publish_safe() timeout while waiting for receipt"
        raise "publish_safe() timeout while waiting for receipt"
      end
    end

    def check_refresh_required
      logger.debug("Checking if we need to refresh connections....")
      stale_connection = false
      current_time = Time.new()
      conn_time = current_time - @connection_start_time
      refresh_connection = false
      if conn_time > @options.conn_lifetime_sec
        stale_connection = true
        logger.info("Stale connection found, need to refresh connection.")
      end

      broken_connection = false
      if @stomp == nil || !@stomp.respond_to?("running") || !@stomp.running
        broken_connection = true
      end


      if @state == STOPPED || stale_connection || broken_connection
        refresh_connection = true
      end
      refresh_connection
    end

    def check_and_refresh_connection
      if check_refresh_required
        logger.debug("Connection status = #{@state}")
        logger.debug("Refreshing connection...")

        if @state != STOPPED
          stop
        end
        start
      end
    end
  
  end
end
