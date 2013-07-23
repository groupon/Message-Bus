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
@queue_name = "jms.queue.testQueue1"
@connection_string = "localhost:61613"

def prepare_configs(ack_type)
  @config = YAML.load_file("../config/messagebus.yml")
  @config["enable_auto_init_connections"] = "true"

  @options = {
    :user => 'guest',
    :passwd => 'guest',
    :subscription_id => 'test2',
    #:ack_type => Messagebus::ACK_TYPE_CLIENT
    #:ack_type => 'client',
    #:ack_type => 'autoClient',
    :ack_type => ack_type,
    :destination_name => 'jms.queue.testQueue1',
    :dynamic_fetch_timeout_ms => 1000,
    :enable_dynamic_serverlist_fetch => true,
    :conn_lifetime_sec => 30
  }
end

def basic_pub_consume
  binary_to_publish = "\xfe\x3e\x5e"
  string_message = "first string message"
  success = false 
  
  json_message = @config.to_json
  cluster = @config["clusters"]

  cluster[0]["producer_address"] = @connection_string
  display "Config: address: #{@queue_name}"

  puts "This test needs manual intervention.."
  puts "kill the broker at localhost:61613"
  puts "waiting for 20 seconds..."
  sleep (20)
  
  client = Messagebus::Client.new(@config.merge(:logger => Logger.new("../mbus.log")))
  client.start
  
  puts "Now start the broker again, waiting for 30 seconds..."
  sleep(30)

  if client.publish @queue_name, string_message
    display "successfully published the string message"
  else
    display "failed to publish"  
  end

  display "Consuming messages now ..."
  cons = Messagebus::Consumer.new(@connection_string, @options)
  cons.start
  print "\n receiving.."  
  message = cons.receive()
  payload = message.payload

  if payload == string_message
    display "successfully received string message"
    success = true
  else
    display "failure while receiving string message"
  end

  client.stop
  cons.stop()
  sucess
end

# Script

prepare_configs('autoClient')
if basic_pub_consume
  puts "Test complete successfully"
end



