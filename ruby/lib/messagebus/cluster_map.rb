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

require 'messagebus/dottable_hash'

module Messagebus
  class ClusterMap
    attr_reader :address, :destinations

    include Validations
    def initialize(config)
      config = DottableHash.new(config)
      Messagebus::Client.logger.debug { "Initializing ClusterMap with config: #{config.inspect}" }

      if clusters = config.clusters
        config.clusters.each do |cluster_config|
          # Merge cluster config with top level config.
          # cluster level values should override top level values.
          cluster = config.merge(cluster_config)
          create_cluster(cluster)
        end
      end
    end

    def start
      cluster_producer_map.each do |cluster_name, producer|
        Messagebus::Client.logger.info "Starting producer for cluster: #{cluster_name} with host_params: #{producer.host_params}"
        producer.start
      end
    end

    def stop
      cluster_producer_map.each do |cluster_name, producer|
        Messagebus::Client.logger.info "Stopping producer for cluster: #{cluster_name} with host_params: #{producer.host_params}"
        if producer.started?
          producer.stop
        else
          Messagebus::Client.logger.warn "#{producer.host_params} was not active, ignoring stop request."
        end
      end
    end

    def find(destination_name)
      destinations[destination_name]
    end

    def destinations
      @destinations ||= {}
    end

    def update_config(config)
      Messagebus::Client.logger.debug { "Reloading ClusterMap with config: #{config.inspect}" }
      config = DottableHash.new(config)
      if clusters = config.clusters
        config.clusters.each do |cluster_config|
          cluster = config.merge(cluster_config)
          #cluster exists - check and update configs
          if cluster_producer_map.has_key?(cluster.name)
            #check for prodcuer config
            update_cluster(cluster)
          else
            #new cluster => create it
            create_cluster(cluster, true)
          end
        end
      end
    end
    
    private
    
    def update_cluster(cluster)
      producer = cluster_producer_map[cluster.name]
      #check for new producer address =>add to exisiting host params list
      #do nothing if producer not found in new config
      cluster_host_params = [cluster.producer_address]  unless cluster.producer_address.is_a?(Array)
      producer_host_params = producer.host_params
      cluster_host_params.each do |address|
        if !producer_host_params.include?(address)
          producer_host_params = producer_host_params.to_a.push address
        end
      end
      producer.host_params=(producer_host_params)
      
      options = producer.options
      options['receipt_wait_timeout_ms'] = cluster.receipt_wait_timeout_ms || options['receipt_wait_timeout_ms']
      options['conn_lifetime_sec'] = cluster.conn_lifetime_sec || options['conn_lifetime_sec']
      
      producer.options=(options)
      
      #load new destination, same producer reference used
      if cluster.destinations && !cluster.destinations.empty?
        cluster.destinations.each do |destination_name|
          load_destination(destination_name, producer)
        end
      else
        raise Client::InitializationError.new("no destinations defined")
      end
    end
    
    def create_cluster(cluster, producer_start = false)
      Messagebus::Client.logger.debug "Initializing cluster: #{cluster.inspect}"
      
      producer = Messagebus::Producer.new(
      cluster.producer_address,
      :user => cluster.user,
      :passwd => cluster.passwd,
      :receipt_wait_timeout_ms => cluster.receipt_wait_timeout_ms,
      :conn_lifetime_sec => cluster.conn_lifetime_sec
      )
      cluster_producer_map[cluster.name] = producer
      
      if cluster.destinations && !cluster.destinations.empty?
        cluster.destinations.each do |destination_name|
          load_destination(destination_name, producer)
        end
      else
        raise Client::InitializationError.new("no destinations defined")
      end
      if producer_start
        producer.start
      end
    end

    def cluster_producer_map
      @cluster_producer_map ||= {}
    end

    def load_destination(destination_name, producer)
      validate_destination_config(destination_name)
      destinations[destination_name] = producer
      Messagebus::Client.logger.info "loaded #{destination_name} => #{producer.host_params}"
    end
  end
end
