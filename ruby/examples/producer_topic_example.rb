#!/usr/bin/ruby

require "rubygems"

$:.unshift(File.join(File.dirname(__FILE__), "..", "lib"))
$:.unshift(File.join(File.dirname(__FILE__), "..", "vendor/gems"))

require "stomp"
require 'messagebus'
require 'thrift'
require 'json'



options = {

  :user => 'rocketman',
  :passwd => 'rocketman',

}

producer = Messagebus::Producer.new("localhost:61613", options)

producer.start

(1..1000).each do |i|

  msg = Messagebus::Message.create("hello world " + i.to_s , nil)

  producer.publish("jms.topic.grouponTestTopic2", "topic", msg);

  sleep 0.100

end


producer.stop
