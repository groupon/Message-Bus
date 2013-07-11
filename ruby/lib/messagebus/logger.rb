require 'logger'

module Messagebus 
  module Logging
    class MyLogger < Logger      
      def error(message, exception=nil)
        if exception
          message += "\n" + exception.message + "\n" + exception.backtrace.join("\n")
        end
        
        super(message)
      end
    end
  end
end
