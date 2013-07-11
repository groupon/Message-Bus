module Messagebus
  class InvalidAcknowledgementType < StandardError; end
  class InvalidDestination < StandardError; end
  class InvalidHost < StandardError; end
  class MessageReceiveTimeout < RuntimeError; end
  class ErrorFrameReceived < RuntimeError; end
end
