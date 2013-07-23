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
@redelivery_delay = 5000

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
  string_message = "first string message"

  cluster = @config["clusters"]
  cluster[0]["producer_address"] = @connection_string
  display "Config: address: #{@queue_name}"

  cons = Messagebus::Consumer.new(@connection_string, @options)
  cons.start
  
  client = Messagebus::Client.new(@config.merge(:logger => Logger.new("../mbus.log")))
  client.start

  if client.publish @queue_name, string_message
    display "successfully published the string message"
  end
  
  display "Consuming messages now ..."  
  print "\n receiving.."
  
  
display "receicing message without ack..."  
  message = cons.receive
  payload = message.payload

  if payload == string_message
    display "successfully received string message"
  else
    display "failure while receiving string message"
  end

  # not acking the first time

  display "trying to receieve again..."
  should_be_empty = cons.receive_immediate()
  
  if !should_be_empty.nil?
    display "Error occurred, message came back immediately"
  else
    display "Receive immediate worked successfully"  
  end
 
  display "Waiting for #{@redelivery_delay} ms..."
  delay = @redelivery_delay/1000
  sleep (delay + 1)
  
  display "receicing message with ack now..."  
  messgae = cons.receive()
  cons.ack()
  payload = message.payload 
  
  if payload == string_message
    display "successfully received string message"
  else
    display "failure while receiving string message"
  end  
  
  display "Waiting for #{@redelivery_delay} ms..."
  sleep (delay + 1)  
  should_be_empty = cons.receive_immediate()
  
  if !should_be_empty.nil?
    display "Error occurred, message came back immediately"
    return false
  end
  
  client.stop
  cons.stop()
  true
end

# Script

prepare_configs('client')
success = basic_pub_consume

if success
  display "Test completed successfully"
end
