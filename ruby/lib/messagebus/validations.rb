module Messagebus
  # :nodoc:all
  module Validations
    def valid_host?(string)
      Messagebus::SERVER_REGEX.match(string)
    end

    def validate_destination_config(name, is_consumer = false, options = {})
      raise InvalidDestination.new("destination name is nil") unless name

      if is_consumer && name.match(/^jms.topic/) && options[:subscription_id].nil?
        raise InvalidDestination.new("destination type TOPIC requires a subscription_id")
      end
    end

    def validate_connection_config(host_params, options = {})
      if options[:ack_type] &&
        options[:ack_type] != Messagebus::ACK_TYPE_AUTO_CLIENT &&
        options[:ack_type] != Messagebus::ACK_TYPE_CLIENT
        raise InvalidAcknowledgementType.new(options[:ack_type])
      end

      if host_params.nil?
        raise InvalidHost.new(host_params)
      end

      if host_params.is_a?(Array)
        host_params.each do |string|
          unless valid_host?(string)
            raise InvalidHost.new("host should be defined as <host>:<port>, received: #{host_params}")
          end
        end
      else
        raise InvalidHost.new("host should be defined as <host>:<port>, received #{host_params}")
      end
    end
  end
end
