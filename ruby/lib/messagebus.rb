$:.unshift(File.join(File.dirname(__FILE__), "..", "vendor/gems"))

require "messagebus/version"
require 'messagebus/logger'
require 'messagebus/message'
require "messagebus/messagebus_types"
require 'messagebus/custom_errors'
require 'messagebus/error_status'
require 'messagebus/validations'
require 'messagebus/connection'
require 'messagebus/producer'
require 'messagebus/consumer'
require 'messagebus/cluster_map'
require 'messagebus/client'

require 'thrift'
require 'base64'
require 'stomp'

module Messagebus
  # These are deprecated and not used anymore
  DEST_TYPE_QUEUE = 'queue'
  DEST_TYPE_TOPIC = 'topic'

  ACK_TYPE_AUTO_CLIENT = 'autoClient'
  ACK_TYPE_CLIENT = 'client'

  LOG_DEFAULT_FILE = '/tmp/messagebus_client.log'

  SERVER_REGEX = /^[a-zA-Z0-9\_.-]+:[0-9]+$/
end

