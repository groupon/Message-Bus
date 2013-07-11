require "spec_helper"

describe Messagebus::Swarm::Controller do
  describe ".after_fork" do
    class SampleWorker ; end

    module AfterForkSpecHelpers
      def temp_file_base
        "after-fork-spec.tmp"
      end

      def create_temp_file(id)
        `touch #{temp_file_base}#{id}`
      end

      def cleanup_temp_files
        `rm #{temp_file_base}* > /dev/null 2>&1`
      end

      def temp_files_count
        `ls -l #{temp_file_base}* | wc -l`.to_i
      end
    end
    include AfterForkSpecHelpers

    before do
      Messagebus::Swarm::Drone.any_instance.stub(:processing_loop => nil)
    end

    around do |example|
      cleanup_temp_files

      original_logger = Messagebus::Swarm::Controller.swarm_control_logger
      @logger = Logger.new(StringIO.new)
      Messagebus::Swarm::Controller.swarm_control_logger = @logger

      example.run

      Messagebus::Swarm::Controller.swarm_control_logger = original_logger

      cleanup_temp_files
    end

    it "executes the after_fork block once per drone started" do
      config = {
        :swarm_config => {:fork => true},
        :workers => [
          {
            :destination => "jms.topic.SampleTopic",
            :worker => "SampleWorker",
            :ack_on_error => false,
            :subscription_id => "sample-subscriber-id",
            :drones => 4
          }
        ],
        :clusters => [
          {
            "consumer_address" => "localhost:61613",
            "destinations" => ["jms.topic.SampleTopic"]
          }
        ]
      }

      Messagebus::Swarm::Controller.after_fork do
        create_temp_file(rand)
      end

      Messagebus::Swarm::Controller.start(config, @logger, "jms.topic.SampleTopic")

      temp_files_count.should == config[:workers].first[:drones]
    end
  end
end
