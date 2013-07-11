module Stomp
  module Error
    class InvalidFormat < RuntimeError
      def message
        "Invalid message - invalid format"
      end
    end

    class InvalidServerCommand < RuntimeError
      def message
        "Invalid command from server"
      end
    end

    class InvalidMessageLength < RuntimeError
      def message
        "Invalid content length received"
      end
    end

    class PacketParsingTimeout < RuntimeError
      def message
        "Packet parsing timeout"
      end
    end
    
    class PacketReceiptTimeout < RuntimeError
      def message
        "Packet receiving timeout"
      end
    end
    
    class MaxReconnectAttempts < RuntimeError
      def message
        "Maximum number of reconnection attempts reached"
      end
    end
  end
end
