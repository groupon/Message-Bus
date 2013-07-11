$:.unshift(File.join(File.dirname(__FILE__), "..", "lib"))
$:.unshift(File.join(File.dirname(__FILE__), "..", "vendor/gems"))

require "stomp"
require 'messagebus'
require 'thrift'

require 'json'
require 'yaml'

require 'tempfile'

require 'messagebus/swarm/controller'

# Prepare for running this by:
#   ssh -f -L 61613:hornetq3.east:61613 -N b.east
#
# Or better yet, setup your local hornet instance:
#  https://wiki.groupondev.com/DPE:Message_Bus_Developer#Setup_local_box_for_development
#
# Then run it as:
#   rspec spec/this_spec.rb
#   less log/drone_run_spec.log

class SimpleDroneRunSpecWorker
  def self.reset!(current_file)
    @current_file = current_file
  end

  # We can't use an instance variable for this because the process receiving
  # the messages won't be the process checking if we've received them
  def self.received_messages(message_id_root)
    return [] if !File.exist?(@current_file)
    messages = []
    File.open(@current_file, "r") do |file|
      file.each_line do |line|
        message = JSON.parse(line)
        messages << message if message['message_id_root'] == message_id_root
      end
    end

    messages
  end

  def self.perform(data)
    # store the response
    File.open(@current_file, "a") do |file|
      file.puts data.to_json
    end
  end
end

describe "A Drone consuming messages" do
  def wait_til_process_finishes(pid)
    loop do
      begin
        Process.getpgid(pid)
        sleep(0.1)
      rescue Errno::ESRCH
        return
      end
    end
  end


  [["processes", {:swarm_config => { :fork => true }}],
   ["threads", {} ]
  ].each do |runner_type, extra_configs|
    context runner_type do
      before do
        @logger = Logger.new("log/drone_run_spec.log")
        @logger.level = 0

        @config = YAML.load_file("config/messagebus_with_drone.yml")
        @config.merge!(extra_configs)

        @file = "#{Dir.tmpdir}/simple_drone_runner_spec-#{Time.now.to_i}"
        SimpleDroneRunSpecWorker.reset!(@file)
      end

      after do
        File.delete(@file) if File.exist?(@file)
      end

      it "receives published messages" do
        # randomly generate a message id so if messages are already sitting on the
        # queue, we don't count them towards this test
        message_id_root = rand(1_000_000_000_000)
        total_published = 3

        # fork off a new controller because that's how it'd work in practice
        # and because we need a PID to tell to stop
        controller_pid = fork do
          Messagebus::Swarm::Controller.start(@config, @logger)
        end

        # fire some messages off
        Messagebus::Client.start(@config.merge(:logger => @logger)) do |client|
          total_published.times do |i|
            client.publish("jms.topic.grouponTestTopic2", {"message_id_root" => message_id_root, "i" => i}).should be_true
          end
        end

        # 30 * 0.1 = max wait 3 seconds
        30.times do
          messages = SimpleDroneRunSpecWorker.received_messages(message_id_root)
          break if messages.size == total_published
          sleep(0.1) # to avoid killing our processor
        end
        # shut down the drones
        Messagebus::Swarm::Controller.stop(controller_pid)
        wait_til_process_finishes(controller_pid)

        SimpleDroneRunSpecWorker.received_messages(message_id_root).size.should == total_published
      end
    end
  end
end