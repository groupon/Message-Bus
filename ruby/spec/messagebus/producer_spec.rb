require File.join(File.dirname(__FILE__), '..', 'spec_helper')

describe Messagebus::Producer do
  def start_test_producer(host_params, fake_stomp=stub(), options = {})
    producer = Messagebus::Producer.new(host_params, options)
    producer.stub!(:check_and_refresh_connection)
    producer.should_receive(:start_server).with(host_params, "", "").and_return(fake_stomp)

    producer
  end

  def test_publish_message(message, options={})
    thrift_base64 = message.to_thrift_binary

    fake_stomp = mock(Stomp::Client)
    header_options = {}
    if options[:header_options]
      header_options = options[:header_options]
    end

    expected_header_options = header_options.merge(Messagebus::Producer::PUBLISH_HEADERS)

    if options[:receipt]
      receipt = stub(Stomp::Message)
      receipt.should_receive(:command).and_return('RECEIPT')
      fake_stomp.should_receive(:publish).with('jms.queue.testqueue', thrift_base64, expected_header_options).and_yield(receipt)
    else
      fake_stomp.should_receive(:publish).with('jms.queue.testqueue', thrift_base64, expected_header_options)
    end

    producer = start_test_producer(['localhost:61613'], fake_stomp, options)
    # Start the producer.
    producer.start

    # Try publishing a simple string message.
    producer.publish('jms.queue.testqueue', message, options[:header_options] || {}, options[:safe])

    # also asserts that we can recreate the message from binary string receive.
    message2 = Messagebus::Message.get_message_from_thrift_binary(thrift_base64)
  end

  before(:each) do
    @msg_string_data1 = 'test data'

    logger = mock(Logger, :info => true, :warn => true, :debug => true)
    Messagebus::Client.stub!(:logger).and_return(logger)
  end

  describe "when starting a producer with multiple hosts" do
    before do
      @hosts = ["localhost:61613"]
      @stomp = mock(Stomp)
    end

    it "starts a stomp server with multiple hosts" do
      producer = Messagebus::Producer.new(@hosts)
      producer.stub!(:refresh_server)
      producer.should_receive(:start_server).with(@hosts, "", "").and_return(@stomp)
      producer.start
    end

    it "starts a stomp server with one host" do
      producer = Messagebus::Producer.new(@hosts.first)
      producer.stub!(:refresh_server)
      producer.should_receive(:start_server).with(@hosts, "", "").and_return(@stomp)
      producer.start
    end
  end  

  describe "publish" do
    it "check publish() with string message." do 
      message = Messagebus::Message.create(@msg_string_data1)
      message2 = test_publish_message(message)
      @msg_string_data1.should == message2.payload
    end
    
    it "check publish() with json message" do
      msg_json_object = {:a => '1', :b => {:x => '2'}}

      message = Messagebus::Message.create(msg_json_object)
      message2 = test_publish_message(message)
      json_string = msg_json_object.to_json
      json_string.should == message2.payload
    end  
    
    it "check publish() with binary message" do
      msg_binary_data = "\xE5\xA5\xBD"

      message = Messagebus::Message.create(msg_binary_data, nil, true)
      message2 = test_publish_message(message)
      msg_binary_data.should == message2.payload
    end 

    context "deprecated dest_type parameter" do
      before do
        @fake_stomp = mock(Stomp::Client)
        @producer = start_test_producer(['localhost:61613'], @fake_stomp)
        @producer.start

        @message = Messagebus::Message.create(@msg_string_data1)
      end

      it "supports not passing dest_type" do
        @fake_stomp.should_receive(:publish).and_yield(stub.as_null_object)
        @producer.publish('jms.queue.testqueue', @message)
      end

      it "supports 5 args with dest_type passed" do
        @fake_stomp.should_receive(:publish).and_yield(stub.as_null_object)
        @producer.publish('jms.queue.testqueue', 'topic', @message, {}, true)
      end

      it "supports 3 args with dest_type passed" do
        @fake_stomp.should_receive(:publish).and_yield(stub.as_null_object)
        @producer.publish('jms.queue.testqueue', 'topic', @message)
      end
    end
  end
  
  it "check publishSafe() with string message fail with timeout." do  
    message = Messagebus::Message.create(@msg_string_data1)

    # Try publishing a simple string message, it should fail with timeout error.
    Messagebus::Client.logger.should_receive(:error).with(/timeout while waiting for receipt/).twice
    message2 = test_publish_message(message, :safe => true, :receipt_wait_timeout_ms => 250, :logger => @logger)
  end 

  it "check publishSafe() with string message" do  
    message = Messagebus::Message.create(@msg_string_data1)
    message2 = test_publish_message(message, :safe=>true, :receipt=>true, :receipt_wait_timeout_ms => 250)
    @msg_string_data1.should == message2.payload
  end    
  
  it "check publishSafe() with string message and scheduled message header" do  
    message = Messagebus::Message.create(@msg_string_data1)
    message2 = test_publish_message(message, :safe=>true, :receipt=>true, :receipt_wait_timeout_ms => 250, 
      :header_options => {Messagebus::Producer::SCHEDULED_DELIVERY_TIME_MS_HEADER => '12345'})
    @msg_string_data1.should == message2.payload
  end 
  
  it "check publish() with string message and scheduled message header" do  
    message = Messagebus::Message.create(@msg_string_data1)
    message2 = test_publish_message(message, :header_options => {Messagebus::Producer::SCHEDULED_DELIVERY_TIME_MS_HEADER => '12345'})
    @msg_string_data1.should == message2.payload
  end 
  
end
