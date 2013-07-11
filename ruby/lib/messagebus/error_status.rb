module Messagebus
  class MessageStatus
    CODES = {
      "1000" => "Invalid Acknowledgement Type",
      "1010" => "Invalid Host",
      "1020" => "Invalid Destination"
    }

    def initialize(code, message, backtrace = nil)
    end
  end
end
