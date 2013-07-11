require 'json'

module Messagebus
  module Swarm
    class Drone
      ##
      # Use this for easy testing that a messagebus message is received.
      # Example config:
      #  -
      #   :destination: jms.topic.some_destination_you_want_to_debug
      #   :subscription_id: <some_subscription_id>
      #   :worker: Messagebus::Swarm::Drone::LoggingWorker
      #   :drones: 1
      class LoggingWorker
        def self.perform_on_destination(message, destination)
          log_message = %|received a message. destination=#{destination}, message=#{message.inspect}|
          Rails.logger.info(log_message) if defined?(Rails.logger)
          puts log_message
        end
      end
    end
  end
end