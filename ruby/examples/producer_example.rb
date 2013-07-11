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
puts "publish as binary"
(1..1000).each do |i|
    client.publish "jms.topic.grouponTestTopic1", binary_to_publish, 0, true, true {"priority" => "4"}
end
print "first round finished!\n"

puts "Publishing as strings"
if RUBY_VERSION.to_f >= 1.9
  binary_to_publish.force_encoding("GB2312")
end

(1..1000).each do |i|
    client.publish "jms.topic.grouponTestTopic1", binary_to_publish
end

client.stop
