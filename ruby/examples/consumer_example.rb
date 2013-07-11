#!/usr/bin/env ruby

# Example Consumer
# This is primitive consumer for messagebus. It uses messagebus library to consume messages from Messagebus
# Unlike producer, this is basic version. Just update your configs in "options" below, And you are good to start
require 'rubygems'
base = File.dirname(__FILE__)
$:.unshift File.expand_path(File.join(base, "..", "lib"))

require "messagebus"
require "yaml"

options = {
  :user => 'rocketman',
  :passwd => 'rocketman',
  :subscription_id => 'lin',
  :ack_type => Messagebus::ACK_TYPE_CLIENT,
  :destination_name => 'jms.topic.grouponTestTopic2',
  :dynamic_fetch_timeout_ms => 1000,
  :enable_dynamic_serverlist_fetch => true,
  :conn_lifetime_sec => 30
}


#config = YAML.load_file("../config/messagebus.yml")
#client = Messagebus::Client.new(config.merge(:logger => Logger.new("../mbus.log")))


# Change this - to meet your local environemnt
cons = Messagebus::Consumer.new("localhost:61613", options)


print "\n starting connection"
cons.start
while true

print "\n receiving.."
message = cons.receive
puts message.payload
cons.ack()
end
# Required, if not in autoClient mode
# cons.ack()

cons.stop


