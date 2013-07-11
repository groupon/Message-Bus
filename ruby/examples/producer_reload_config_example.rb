#!/usr/bin/env ruby
require "rubygems"
base = File.dirname(__FILE__)
$:.unshift File.expand_path(File.join(base, "..", "lib"))

require "irb"
require "yaml"
require "messagebus"

binary_to_publish = "\xfe\x3e\x5e"
config = YAML.load_file("../config/messagebus.yml")
client = Messagebus::Client.new(config.merge(:logger => Logger.new("../mbus.log")))
client.start
(1..10).each do |i|
    client.publish "jms.topic.grouponTestTopic1", binary_to_publish
end
print "first round finished!\n"
sleep 5
config = YAML.load_file("../config/messagebus_modified.yml")
client.reload_config_on_interval(config, 0)
print "second round starting!\n" 

(1..10).each do |i|
    client.publish "jms.topic.grouponTestTopic2", binary_to_publish
end

client.stop
