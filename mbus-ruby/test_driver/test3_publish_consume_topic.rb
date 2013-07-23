#!/usr/bin/env ruby

base = File.dirname(__FILE__)
$:.unshift File.expand_path(File.join(base, "..", "lib"))

require "irb"
require "yaml"
require "messagebus"
require "thrift"
require "json"

def display(data)
  puts "\n #{data}"
end

# ADJUST PARAMS HERE
@queue_name = "jms.topic.testTopic1"
@connection_string = "localhost:61613"

def prepare_configs(ack_type)
  @config = YAML.load_file("../config/messagebus.yml")
  @config["enable_auto_init_connections"] = "true"

  client = Messagebus::Client.new(@config.merge(:logger => Logger.new("../mbus.log")))
  client.start

  @options = {
    :user => 'guest',
    :passwd => 'guest',
    :subscription_id => 'test4',
    #:ack_type => Messagebus::ACK_TYPE_CLIENT
    #:ack_type => 'client',
    #:ack_type => 'autoClient',
    :ack_type => ack_type,
    :destination_name => @queue_name,
    :dynamic_fetch_timeout_ms => 1000,
    :enable_dynamic_serverlist_fetch => true,
    :conn_lifetime_sec => 30
  }
end

def basic_pub_consume
  cons = Messagebus::Consumer.new(@connection_string, @options)
  cons.start

  string_message = "test message"

  puts "created subsc #{@options[:subscription_id]}"

  client = Messagebus::Client.new(@config.merge(:logger => Logger.new("../mbus.log")))
  client.start

  sent_success = client.publish @queue_name, string_message
  if sent_success == true
    display "successfully published the string message"
  end

  message = cons.receive
  payload = message.payload

  if payload == string_message
    display "successfully received string message"
  else
    display "failure while receiving string message"
  end

  if(@options[:ack_type] == "client")
    display "Acking..."
    cons.ack()
  end

  cons.stop()
  client.stop()
end

# Script
prepare_configs('client')
basic_pub_consume
