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
  :subscription_id => 'test'
}

producer = Messagebus::Producer.new("localhost:61613", options)

producer.start

(1..10).each do |i|
  print "\n sending #{i}"
  msg = Messagebus::Message.create_string_message("hello world " + i.to_s , nil)
  t1 = Time.now.to_f * 1000 + i * 5000
  producer.publish_safe("jms.topic.grouponTestTopic1","topic", msg, {
    Messagebus::Producer::SCHEDULED_DELIVERY_TIME_MS_HEADER => t1.to_i.to_s
  });
end

producer.stop
