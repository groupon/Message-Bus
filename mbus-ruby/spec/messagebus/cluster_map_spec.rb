require File.join(File.dirname(__FILE__), '..', 'spec_helper')

describe Messagebus::ClusterMap do
  before do
    @config = {
      "user" => "RandySavage",
      "passwd" => "snap.into.a.slim.jim",
      "clusters" => ["name" => "producer-cluster", "producer_address" => "localhost:61613", "destinations" => [
        "jms.queue.testQueue1", "jms.topic.testTopic1"]
      ]
    }

    logger = mock(Logger, :info => true, :debug => true)
    Messagebus::Client.stub!(:logger).and_return(logger)
  end

  describe "constructing a cluster map" do
    before do
      @producer = mock(Messagebus::Producer, :host_params => true)
      Messagebus::Producer.stub!(:new).and_return(@producer)
    end

    it "creates a producer for each cluster configured" do
      cluster = @config["clusters"].first
      cluster_config = cluster.dup
      host_params = cluster_config.delete("producer_address")

      Messagebus::Producer.should_receive(:new).with(host_params, hash_including({
        :user => "RandySavage", :passwd => "snap.into.a.slim.jim", :receipt_wait_timeout_ms => nil,
        :conn_lifetime_sec => nil
      }))
      Messagebus::ClusterMap.new(@config)
    end

    it "creates a destination for each cluster destination name configured" do
      expected_map = {
        "jms.queue.testQueue1" => @producer,
        "jms.topic.testTopic1" => @producer
      }

      clustermap = Messagebus::ClusterMap.new(@config)
      expected_map.should == clustermap.destinations
      clustermap.destinations.each do |destination, producer|

      end
    end

    it "fails with exception if no destinations are configured" do
      lambda {
        @config["clusters"].first.merge!("destinations" => [])
        Messagebus::ClusterMap.new(@config)
      }.should raise_error(Messagebus::Client::InitializationError)

      lambda {
        @config["clusters"].first.merge!("destinations" => nil)
        Messagebus::ClusterMap.new(@config)
      }.should raise_error(Messagebus::Client::InitializationError)
    end
  end

  describe "#start" do
    before do
      @producer = mock(Messagebus::Producer, :host_params => true)
      Messagebus::Producer.stub!(:new).and_return(@producer)
    end

    it "producer start is called" do
      @producer.should_receive(:start)
      Messagebus::ClusterMap.new(@config).start
    end
  end

  describe "update clusters with top level configurations" do
    before do
      @producer = mock(Messagebus::Producer, :host_params => true)
      Messagebus::Producer.stub!(:new).and_return(@producer)

      @config = {
        "user" => "top-level-name",
        "clusters" => ["name" => "producer-cluster", "producer_address" => "localhost:61613", "destinations" => [
          "jms.queue.testQueue1", "jms.topic.testTopic1"]
        ]
      }
    end

    it "creates a producer for each cluster configured with top level name." do
      cluster = @config["clusters"].first
      cluster_config = cluster.dup
      host_params = cluster_config.delete("producer_address")

      Messagebus::Producer.should_receive(:new).with(host_params, hash_including({
        :user => 'top-level-name', :passwd => nil, :receipt_wait_timeout_ms => nil,
        :conn_lifetime_sec => nil
      }))
      Messagebus::ClusterMap.new(@config)
    end

    it "creates a producer for each cluster configured with cluster level name." do
      @config["clusters"].first.merge!({"user" => 'cluster-level-name'})
      cluster = @config["clusters"].first
      cluster_config = cluster.dup
      host_params = cluster_config.delete("producer_address")

      Messagebus::Producer.should_receive(:new).with(host_params, hash_including({
        :user => 'cluster-level-name', :passwd => nil, :receipt_wait_timeout_ms => nil,
        :conn_lifetime_sec => nil
      }))
      Messagebus::ClusterMap.new(@config)
    end
  end

  describe "#stop" do
    before do
      @producer = mock(Messagebus::Producer, :host_params => true)
      Messagebus::Producer.stub!(:new).and_return(@producer)
      @producer.stub!(:started?).and_return(true)
    end

    it "producer stop is called" do
      @producer.should_receive(:stop)
      Messagebus::ClusterMap.new(@config).stop
    end
  end

  describe "#find" do
    before do
      @producer = mock(Messagebus::Producer, :host_params => true)
      Messagebus::Producer.stub!(:new).and_return(@producer)
    end

    it "succeeds in finding a destination by name" do
      Messagebus::ClusterMap.new(@config).find("jms.queue.testQueue1").should == @producer
    end
  end
  
  describe "#update_config" do
    before do
      @producer = Messagebus::Producer.new(
              "localhost:61613",
              :user => "RandySavage",
              :passwd => "snap.into.a.slim.jim" 
              )
      Messagebus::Producer.stub!(:new).and_return(@producer) 
    end
    
    it "update_cluster is called" do
      clustermap = Messagebus::ClusterMap.new(@config)
      cluster_config = @config["clusters"].first
      cluster = @config.merge(cluster_config)
      clustermap.should_receive(:update_cluster).with(cluster)
      clustermap.update_config(@config)
    end
    
    it "succeeds in loading new destiantions from config" do
      clustermap = Messagebus::ClusterMap.new(@config)
      @config["clusters"].first["destinations"].push("jms.topic.testTopic2")
      clustermap.update_config(@config)
      expected_map = {
             "jms.queue.testQueue1" => @producer,
             "jms.topic.testTopic1" => @producer,
             "jms.topic.testTopic2" => @producer
           }
      expected_map.should == clustermap.destinations
    end
    
   
    it "succeeds in creating new clusters" do
      clustermap = Messagebus::ClusterMap.new(@config)
      @config["clusters"].push("name" => "customer-cluster", "producer_address" => "localhost:61613", "destinations" => [
        "jms.queue.testQueue1", "jms.topic.testTopic1"]
      )
      cluster_config = @config["clusters"][1]
      cluster = @config.merge(cluster_config)
      clustermap.should_receive(:create_cluster).with(cluster,true)
      clustermap.update_config(@config)
    end
  end
end 
