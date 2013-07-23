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

require 'messagebus/consumer'
require 'messagebus/swarm/drone'

require 'yaml'

module Messagebus
  module Swarm
    ##
    # The controller for a set of drone workers.
    class Controller
      ConfigurationSource = Struct.new(:configuration_type, :value) do
        def configuration_hash
          case configuration_type
          when :path
            YAML.load_file(value)
          when :eval
            eval(value)
          when :default
            Messagebus::Swarm.default_configuration
          else
            raise "Unsupported configuration type #{configuration_type}"
          end
        end
      end

      class BadConfigurationError < RuntimeError; end

      # It's important this a different logger instance than the one used for
      # the drones/consumers/other things to avoid deadlocking issues. It's ok
      # for it to use the same file, just not be the same instance of a logger.
      #
      # This logger will be used in a signal handler, and logging involves
      # mutexes, so we need/want to be sure the logger isn't being used by
      # any other code outside the signal handler.
      def self.swarm_control_logger=(swarm_control_logger)
        @swarm_control_logger = swarm_control_logger
      end
      def self.swarm_control_logger
        @swarm_control_logger ||= Logger.new($stdout)
      end

      def self.after_fork(&block)
        after_fork_procs << block
      end

      def self.after_fork_procs
        @after_fork_procs ||= []
      end

      ##
      # Starts up the swarm based on the given config. This method does not
      # return until the swarm is stopped down.
      #
      # If the config has config[:swarm][:fork]=true, it will boot the drones
      # in subprocesses, otherwise it will use threads.
      #
      # [destination_name] limit booting drones to only ones acting on the given
      # destination
      # [drone_count] override the number of drones to run
      def self.start(configuration_source, drone_logger, destination_name=nil, drone_count=nil)
        config = if configuration_source.is_a?(ConfigurationSource)
          configuration_source.configuration_hash
        else
          configuration_source
        end
        raise BadConfigurationError.new("#{configuration_source.inspect} didn't evaluate to a configuration") if config.nil?
        config = Messagebus::DottableHash.new(config)
        relevant_worker_configs = config.workers

        # apply any applicable destination_name or drone_count settings
        relevant_worker_configs = relevant_worker_configs.select { |worker_config| worker_config[:destination] == destination_name } if destination_name
        relevant_worker_configs = relevant_worker_configs.map { |worker_config| worker_config.merge(:drones => drone_count) } if drone_count

        # The || is for backwards compatibility
        default_cluster_config = config.cluster_defaults || config

        drones = build_drones(relevant_worker_configs, default_cluster_config, config.clusters, swarm_control_logger, drone_logger)
        booter = start_drones(swarm_control_logger, config.swarm_config && config.swarm_config.fork, drones)

        booter.wait
      end

      ##
      # Shut down a previously started swarm
      def self.stop(pid)
        stop_drones(pid)
      end

      def self.require_files(files)
        files.each { |file_to_require| require file_to_require }
      end

      def self.write_pid(pid_file)
        File.open(pid_file, "w") { |f| f.print(Process.pid) }
      end

      def self.delete_pid(pid_file)
        File.delete(pid_file) if File.exist?(pid_file)
      end

      # :nodoc: all
      module ProcessManagementConcerns
        # We capture INT (ctrl-c) and TERM (default signal kill sends).
        STOP_PARENT_PROCESSING_SIGNALS = %w(INT TERM)
        STOP_SUBPROCESS_SIGNAL = 'TERM'

        class DroneForker
          def initialize(drones)
            @drones = drones
          end

          def start
            # Intentionally not doing this with a map so if we get a signal
            # while building this list up, we'll still have some/most of the
            # pids. This still isn't perfect. Sure there's a perfect way to
            # do this.
            @pids = []
            parent_pid = Process.pid

            @drones.map do |drone|
              @pids << fork do
                $0 = "ruby messagebus_swarm worker for #{parent_pid}-#{drone.id}"
                Messagebus::Swarm::Controller.after_fork_procs.each{|after_fork_proc| after_fork_proc.call }
                register_stop_signal_handler(drone)
                drone.processing_loop
              end
            end
          end

          def wait
            Process.wait
          end

          def stop
            @pids.each do |pid|
              Process.kill(STOP_SUBPROCESS_SIGNAL, pid)
            end
          end

          private
          def register_stop_signal_handler(drone)
            Signal.trap(STOP_SUBPROCESS_SIGNAL) do
              # The drone is stopped in a separate thread so that when our
              # signal handler triggers, it'll be unblocking a thread that is
              # not itself. Without this, ruby's queue impl seems to get stuck.
              # This most likely has something to do with a thread unblocking
              # itself, which isn't something that could ever happen outside
              # of a signal.
              # tldr: this is black magic, and seems to work
              Thread.new do
                drone.stop
              end
            end
          end
        end

        class DroneThreader
          def initialize(drones)
            @drones = drones
          end

          def start
            @threads = @drones.map do |drone|
              Thread.new do
                drone.processing_loop
              end
            end
          end

          def wait
            @threads.each(&:join)
          end

          def stop
            @drones.each(&:stop)
          end
        end

        def stop_drones(pid)
          Process.kill(STOP_PARENT_PROCESSING_SIGNALS[0], pid)
        end

        def start_drones(swarm_control_logger, fork_or_not, drones)
          swarm_control_logger.info("Booting drones fork=#{!!fork_or_not}")
          booter = if fork_or_not
            DroneForker.new(drones)
          else
            DroneThreader.new(drones)
          end

          STOP_PARENT_PROCESSING_SIGNALS.each do |signal|
            Signal.trap(signal) do
              swarm_control_logger.info("Stopping drones")
              booter.stop
            end
          end
          booter.start
          booter
        end
      end
      extend ProcessManagementConcerns

      # :nodoc: all
      module DroneFactoryConcerns
        def build_drones(worker_configs, default_cluster_config, cluster_configs, swarm_control_logger, drone_logger)
          drones = worker_configs.map do |worker_config|
            destination_name = worker_config.destination
            relevant_clusters = cluster_configs.select do |cluster_config|
              destinations = cluster_config.destinations
              destinations.include?(destination_name)
            end

            if relevant_clusters.empty?
              swarm_control_logger.warn "couldn't find any clusters to process #{destination_name}"
            end

            consumers = build_consumers(worker_config, relevant_clusters, destination_name, default_cluster_config, swarm_control_logger)
            drones = build_drone_instances(worker_config, consumers, destination_name, drone_logger)
          end.flatten

          drones
        end

        private
        def build_consumers(worker_config, relevant_clusters, destination_name, default_cluster_config, swarm_control_logger)
          relevant_clusters.map do |cluster_config|
            full_cluster_config = default_cluster_config.merge(cluster_config)
            consumer_address = full_cluster_config.consumer_address

            swarm_control_logger.debug { "creating a consumer for #{destination_name}@#{consumer_address} with settings: #{full_cluster_config.inspect}" }

            # Build worker_count consumers because we should have 1 consumer per drone
            (worker_config.drones || 1).times.map do
              Messagebus::Consumer.new([consumer_address], full_cluster_config.merge!(
                :destination_name => destination_name,
                :ack_type => Messagebus::ACK_TYPE_CLIENT,
                :subscription_id => worker_config.subscription_id
              ))
            end
          end.flatten
        end

        def build_drone_instances(worker_config, consumers, destination_name, drone_logger)
          consumers.map do |consumer|
            worker_class = constantize(worker_config.worker)

            # arbitrary id, but seems like having a unique identifier for the drone could come in handy
            drone_id = rand(1_000_000_000).to_s
            drone = Drone.new(:consumer => consumer,
                              :destination_name => destination_name,
                              :id => drone_id,
                              :logger => drone_logger,
                              :worker_class => worker_class)
          end
        end

        def constantize(worker_class_name)
          worker_class_name.split("::").inject(Object) do |base_response, class_string|
            base_response = base_response.const_get(class_string)
          end
        end
      end
      extend DroneFactoryConcerns
    end
  end
end