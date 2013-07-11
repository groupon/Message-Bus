require File.join(File.dirname(__FILE__), '..', 'spec_helper')

describe Messagebus::Client do
  before do
    @config = {
      "enable_auto_init_connections" => true,
      "clusters" => [
        {
          "type" => "producer-cluster",
          "address" => "localhost:61613",
          "destinations" => [
            "jms.queue.grouponTestQueue1",
            "jms.topic.grouponTestTopic1"
          ]
        }
      ]
    }

    @logger = mock(Logger, :info => true, :error => true, :debug => true, :warn => true)
    Logger.stub(:new).and_return(@logger)

    @cluster_map = mock(Messagebus::ClusterMap, :start => true, :stop => true)
    Messagebus::ClusterMap.stub!(:new).and_return(@cluster_map)
    
  end

  describe "constructing a client" do
    describe "when creating a singleton logger" do
      it "creates a logger using tmp directory if configuration is omitted" do
        Messagebus::Client.new(@config)
        Messagebus::Client.logger.should == @logger
      end

      it "creates a logger with the supplied log_file" do
        Logger.should_receive(:new).with("log/messagebus-client.log").and_return(@logger)
        Messagebus::Client.new(@config.merge("log_file" => "log/messagebus-client.log"))
      end
    end

    it "automatically starts the cluster if configured" do
      @cluster_map.should_receive(:start)
      Messagebus::Client.new(@config.merge("enable_auto_init_connections" => true)).start
    end

    it "automatically defaults to cluster not started" do
      @cluster_map.should_not_receive(:start)
      Messagebus::Client.new(@config)
    end
  end

  describe ".start (class method)" do
    it "auto closes the connection when a block is given" do
      @cluster_map.should_receive(:start)
      @cluster_map.should_receive(:stop)
      Messagebus::Client.start(@config.merge("enable_auto_init_connections" => true)) {}
    end

    it "makes sure it stops if the block errors out" do
      @cluster_map.should_receive(:start)
      @cluster_map.should_receive(:stop)
      proc do
        Messagebus::Client.start(@config.merge("enable_auto_init_connections" => true)) do
          raise "error123"
        end
      end.should raise_error("error123")
    end

    it "doesn't auto close if no block passed" do
      @cluster_map.should_receive(:start)
      @cluster_map.should_not_receive(:stop)
      Messagebus::Client.start(@config.merge("enable_auto_init_connections" => true))

    end
  end

  describe "#start" do
    it "delegates to the cluster map start" do
      @cluster_map.should_receive(:start)
      @cluster_map.should_not_receive(:stop)
      Messagebus::Client.new(@config.merge("enable_auto_init_connections" => true)).start
    end
  end

  describe "#stop" do
    it "delegates to the cluster map stop" do
      @cluster_map.should_receive(:stop)
      Messagebus::Client.new(@config).stop
    end
  end

  describe "#logger" do
    it "shares the logger across instances" do
      Messagebus::Client.new(@config).logger.should be(Messagebus::Client.new(@config).logger)
    end
  end

  describe "#publish" do
    before do
      @producer  = mock(Messagebus::Producer, :publish => true)
      @cluster_map.stub!(:find).and_return(@producer)
      @producer.stub!(:started?).and_return(true)

      @message = mock(Messagebus::Message, :create => true, :message_id => "ee4ae017f409dc3f4aad38dddeb7bf88")
      Messagebus::Message.stub!(:create).and_return(@message)
    end

    it "creates a message from the provided object" do
      Messagebus::Message.should_receive(:create).with({:benjamin => :franklins})
      Messagebus::Client.new(@config).publish("jms.queue.Testqueue1", {:benjamin => :franklins})
    end

    it "requires an existing destination" do
      @producer.should_not_receive(:publish)
      lambda {
        @cluster_map.stub!(:find).and_return(nil)
        Messagebus::Client.new(@config).publish("jms.queue.grouponTestQueue1", {:benjamin => :franklins})
      }.should raise_error(Messagebus::Client::InvalidDestinationError)
    end

    it "sends a safe message by default" do
      @producer.should_receive(:publish)
      Messagebus::Client.new(@config).publish("jms.queue.grouponTestQueue1", {:benjamin => :franklins})
    end

    it "sends an unsafe message by when the safe parameter is false" do
      @producer.should_receive(:publish)
      Messagebus::Client.new(@config).publish("jms.queue.grouponTestQueue1", {:benjamin => :franklins}, 0, false)
    end
    
    it "sends a message with headers" do
      headers = {"priority" => 6}
      @producer.should_receive(:publish).with("jms.queue.grouponTestQueue1", @message, headers, false)
      Messagebus::Client.new(@config).publish("jms.queue.grouponTestQueue1", {:benjamin => :franklins}, 0, false, false, headers)
    end
  end
  
  describe "#reload_config_on_interval" do 
   it "reloads config when the interval is reached" do
     @cluster_map.should_receive(:update_config).with(@config)
     Messagebus::Client.new(@config).reload_config_on_interval(@config,0)
   end
  end

  describe "#headers" do
    it "creates a header from current time and the delay milliseconds provided to publish" do
      delay = 3
      time = Time.now
      Time.stub!(:now).and_return(time)

      client = Messagebus::Client.new(@config)
      client.headers(delay).should == {
        Messagebus::Producer::SCHEDULED_DELIVERY_TIME_MS_HEADER => ((time.to_i * 1000) + delay).to_s
      }
    end
  end
end

