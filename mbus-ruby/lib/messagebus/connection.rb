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

require "messagebus/validations"
require "messagebus/dottable_hash"

module Messagebus
  class Connection

    STARTED = "STARTED"
    STOPPED = "STOPPED"

    attr_accessor :host_params, :options

    include Validations

    def initialize(host_params, passed_options = {})
      @host_params = host_params
      @host_params = [@host_params] unless @host_params.is_a?(Array)

      @options = DottableHash.new({
        :user => '', :passwd => '',
        :conn_lifetime_sec => 300, :receipt_wait_timeout_ms => 5000,
        :destination_name => nil, :destination_type => nil,
        :ack_type => Messagebus::ACK_TYPE_AUTO_CLIENT, :num_threads_per_server => 1,
        :enable_dynamic_serverlist_fetch => false, :dynamic_fetch_timeout_ms => 1000,
        :dynamic_serverlist_fetch_url_override => nil
      }).merge(passed_options)

      @state = STOPPED
    end

    def started?
      @state == STARTED
    end

    def stopped?
      @state == STOPPED
    end

    def do_with_timeout(timeout_ms)
      if not block_given?
        raise "do_with_timeout expects a block to be run"
      end

      start_time = Time.now
      while (Time.now - start_time) * 1000 < timeout_ms
        yield
      end
    end

    def start_server(host_params, user, passwd, subscription_id=nil)
      case host_params
      when Array
        host_param = host_params[rand(host_params.length)]
      when String
        host_param = host_params
      end

      host, port = host_param.split(':')

      connect_headers = {}
      connect_headers.merge!("client-id" => subscription_id) if subscription_id

      stomp = Stomp::Client.new(user, passwd, host, port, logger,  connect_headers)
      logger.info "Started client for host_param:#{host_param} stomp-client:#{stomp} user:#{user}"
      @state = STARTED

      return stomp
    end

    def stop_server(stomp)
      Client.logger.info "Stopping stomp-client:#{stomp}"
      stomp.close if stomp
      @state = STOPPED
    end

    def host_params=(host_params)
      @host_params = host_params
    end
     
    def options=(options)
      @options = options
    end 
    private

    def logger
      @logger ||= Client.logger
    end
  end
end
