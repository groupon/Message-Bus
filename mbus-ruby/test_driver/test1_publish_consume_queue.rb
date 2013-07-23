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

  json_message = @config.to_json
  cluster = @config["clusters"]

  cluster[0]["producer_address"] = @connection_string
  display "Config: address: #{@queue_name}"

  client = Messagebus::Client.new(@config.merge(:logger => Logger.new("../mbus.log")))
  client.start

  sent_success = client.publish @queue_name, string_message
  if sent_success == true
    display "successfully published the string message"
  end

  if client.publish @queue_name, binary_to_publish
    display "successfully published the binary message"
  end

  if client.publish @queue_name, json_message
    display "successfully published the json message"
  end

  display "Consuming messages now ..."
  cons = Messagebus::Consumer.new(@connection_string, @options)
  cons.start
  print "\n receiving.."
  for i in (1..3)  do
    print "\n receiving(#{i}).."
    message = cons.receive
    payload = message.payload
    case i
    when 1
      if payload == string_message
        display "successfully received string message"
      else
        display "failure while receiving string message"
      end
    when 2
      if payload == binary_to_publish
        display "successfully received binary message"
      else
        display "failure while receiving binary message"
      end
    when 3
      if payload == @config.to_json
        display "successfully received json messsage"
      else
        display "failure while receiving json message"
      end          
    end
    if(@options[:ack_type] == "client")
          display "Acking..."
          cons.ack()      
    end
  end

  client.stop
  cons.stop()
end

# Script

prepare_configs('autoClient')
basic_pub_consume

prepare_configs('client')
basic_pub_consume
