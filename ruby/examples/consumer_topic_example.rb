#!/usr/bin/ruby

require "rubygems"

$:.unshift(File.join(File.dirname(__FILE__), "..", "lib"))
$:.unshift(File.join(File.dirname(__FILE__), "..", "vendor/gems"))

require "stomp"
require 'messagebus'
require 'thrift'
require 'json'


options= {
  :user => 'rocketman',
  :passwd => 'rocketman',
  :subscription_id => 'lin',
  :ack_type => Messagebus::ACK_TYPE_CLIENT,
  
  :destination_name => 'jms.topic.grouponTestTopic2',
  :dynamic_fetch_timeout_ms => 1000,
  :enable_dynamic_serverlist_fetch => false,
  :conn_lifetime_sec => 5
  
}


#config = YAML.load_file("../config/messagebus.yml")
#client = Messagebus::Client.new(config.merge(:logger => Logger.new("../mbus.log")))


# Change this - to meet your local environemnt
cons = Messagebus::Consumer.new("localhost:61613", options)

print "\n starting connection"
cons.start()
print "\n receiving.."
sleep 1
(1..10000).each do |i|
  message = cons.receive
  sleep 0.001
  
  if message != nil
    print "#{message.payload}\n."
    cons.ack()
  else
    puts 'message not found.'
  end
end
