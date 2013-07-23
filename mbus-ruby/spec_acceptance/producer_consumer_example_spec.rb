$:.unshift(File.join(File.dirname(__FILE__), "..", "lib"))
$:.unshift(File.join(File.dirname(__FILE__), "..", "vendor/gems"))

require "stomp"
require 'messagebus'
require 'thrift'

require 'json'
require 'yaml'

# Prepare for running this by:
#   ssh -f -L 61613:hornetq3.east:61613 -N b.east
#
# Or better yet, setup your local hornet instance:
#  https://wiki.groupondev.com/DPE:Message_Bus_Developer#Setup_local_box_for_development
#
# Then run it as:
#   rspec spec_integration/this_spec.rb
#   less mbus_end_to_end.log

describe "A Consumer and Producer" do
  before do
    @config = YAML.load_file("config/messagebus.yml")
  end

  it "receives published messages" do
    message_id_root = rand(1_000_000_000_000)

    finished_consuming = false

    initial_time = Time.now
    max_time = initial_time + 5 # max test duration is 5 seconds

    total_published = 3
    total_received = 0

    consumer_thread = Thread.new do
      consumer_config = @config.merge(
        :subscription_id => 'producer_consumer_example_spec_id',
        :ack_type => 'autoClient',
        :destination_name => 'jms.topic.testTopic2',
        :dynamic_fetch_timeout_ms => 1000,
        :enable_dynamic_serverlist_fetch => true,
        :user => @config["user"],
        :passwd => @config["passwd"]
      )
      Messagebus::Consumer.start("localhost:61613", consumer_config) do |consumer|
        loop do
          message = consumer.receive_immediate
          if !message
            sleep(0.1)
          elsif JSON.parse(message.payload)["message_id_root"] == message_id_root
            total_received += 1
          else
            # disregard this message
          end

          if total_received == total_published
            break # success
          elsif Time.now > max_time
            break # abort
          end
        end

        finished_consuming = true
      end
    end

    Messagebus::Client.start(@config.merge(:logger => Logger.new("log/mbus_end_to_end.log"))) do |client|
      total_published.times do |i|
        client.publish("jms.topic.testTopic2", {"message_id_root" => message_id_root, "i" => i}).should be_true
      end
    end

    consumer_thread.join

    total_received.should == total_published
  end
end
