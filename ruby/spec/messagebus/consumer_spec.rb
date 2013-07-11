require File.join(File.dirname(__FILE__), '..', 'spec_helper')

require 'thread'
require 'net/http'

describe Messagebus::Consumer do
  DYNAMIC_URL = "http://localhost:8081/jmx?command=get_attribute&args=org.hornetq%3Amodule%3DCore%2Ctype%3DServer%20ListOfBrokers"

  def start_test_consumer(host_params, options = {})
    consumer = Messagebus::Consumer.new(host_params, options.merge({
     :destination_name => 'jms.queue.testqueue'
    }))

    if options[:any_host_params]
      consumer.should_receive(:start_servers).at_least(1)
    else
      consumer.should_receive(:start_servers).with(host_params, true).at_least(1)
    end

    consumer
  end

  def test_receive_message(host_params, message, options = {})
    consumer = start_test_consumer(host_params, options)
    fakeClient = mock(Stomp::Client)

    expected_message = options[:expected_message]
      
    if not expected_message.nil?
      fakeClient.should_receive(:credit).with(message)
    end
     
    consumer.servers_running[host_params[0]] = fakeClient 
    if not message.nil?
      if not options[:push_message_delay]
        consumer.received_messages.push({:msg => message, :host_param => host_params[0], :decoded_msg => expected_message})
      else
        Thread.start do
          sleep options[:push_message_delay]
          consumer.received_messages.push({:msg => message, :host_param => host_params[0], :decoded_msg => expected_message})
        end
      end
    end

    consumer.start

    if options[:receive] == 'blocking'
        received_message = consumer.receive
    end

    if  options[:receive] == 'timeout'
      received_message = consumer.receive_timeout(options[:timeout_ms] || 100)
    end

    if  options[:receive] == 'immediate'
      received_message = consumer.receive_immediate
    end

    if(received_message != nil && options[:ack_type] == Messagebus::ACK_TYPE_AUTO_CLIENT)
      fakeClient.should_receive(:acknowledge).with(message)
    end

    if( options[:will_call_ack])
      fakeClient.should_receive(:acknowledge).with(message)
    end

    if( options[:will_call_nack])
      fakeClient.should_receive(:nack).with(message)
    end
    
    if( options[:will_call_keepalive])
      fakeClient.should_receive(:keepalive)
    end

    if( options[:will_call_ack_safe])
      receipt = stub(Stomp::Message)
      receipt.should_receive(:command).and_return('RECEIPT')
      fakeClient.should_receive(:acknowledge).with(message).and_yield(receipt)
      #fakeClient.should_receive(:acknowledge).and_yield
    end

    received_message.class.should == expected_message.class
    if received_message.class == Messagebus::Message
      received_message.raw_message.should == expected_message.raw_message
    else
      received_message.should == expected_message
    end

    consumer
  end

  before(:each) do
    @message = Messagebus::Message.create("hello world1")
    @msg = Stomp::Message.new(nil)
    @msg.body = @message.to_thrift_binary
    @msg_id = 'id-1'
    @msg_string_data1 = 'test data'
    @msg_json_object = {:a=> '1', :b => {:x => '2'}}
    @msg_binary_data = "\xE5\xA5\xBD"

    logger = mock(Logger, :info => true, :error => true)
    Messagebus::Client.stub!(:logger).and_return(logger)
  end

  describe ".start (class method)" do
    before do
      @consumer = Messagebus::Consumer.new(['localhost:61613'], :destination_name => 'jms.queue.testqueue')
      @consumer.should_receive(:start_servers)
      Messagebus::Consumer.stub!(:new).and_return(@consumer)
    end

    it "auto closes the connection when a block is given" do
      Messagebus::Consumer.start(['localhost:61613']) {}
      @consumer.stopped?.should be_true
    end

    it "makes sure it stops if the block errors out" do
      proc do
        Messagebus::Consumer.start(['localhost:61613']) do
          raise "error123"
        end
      end.should raise_error("error123")
      @consumer.stopped?.should be_true
    end

    it "doesn't auto close if a block isn't given" do
      Messagebus::Consumer.start(['localhost:61613'])
      @consumer.stopped?.should be_false
      @consumer.started?.should be_true
    end 
  end

  describe "start" do
    it "starts a server" do
      consumer = start_test_consumer(['localhost:61613'])
      consumer.start
      consumer.stopped?.should be_false
      consumer.started?.should be_true
    end

    it "start server attached to multiple hosts" do
      consumer = start_test_consumer(['localhost:61613', 'localhost:61614'])
      consumer.start
    end
  end

  context "connection failure" do
    it "closes connections when only some start" do
      consumer = Messagebus::Consumer.new(['host1:1', 'host2:2'], :destination_name => 'jms.queue.testqueue')
      lambda do
        good_client = stub.as_null_object
        good_client.should_receive(:close)

        Stomp::Client.should_receive(:new).and_return(good_client)
        Stomp::Client.should_receive(:new).and_raise("2nd client blows up")
        consumer.start
      end.should raise_error("2nd client blows up")
    end

    it "raises an exception when we fail to connect" do
      consumer = Messagebus::Consumer.new(['non_existant_host:1111'], :destination_name => 'jms.queue.testqueue')
      lambda do
        consumer.start
      end.should raise_error
    end
  end

  it "check refresh timer task" do
    consumer = start_test_consumer(['localhost:61613', 'localhost:61614'], :conn_lifetime_sec => 0.100)
    consumer.should_receive(:refresh_servers).at_least(1)
    consumer.start
    sleep(0.500)
  end
  
  it "check refresh timer task with dynamic fetch url return nil." do
    consumer = start_test_consumer(['localhost:61613', 'localhost:61614'], :conn_lifetime_sec => 0.100,
    :enable_dynamic_serverlist_fetch => 'true', :dynamic_serverlist_fetch_url_override => 'test', :any_host_params=>true)
    consumer.should_receive(:fetch_serverlist).at_least(1).and_return(nil)
    consumer.start
    sleep(0.200)
  end

  it "check get dynamic fetch url host param String." do
    consumer = start_test_consumer(['localhost:61613', 'localhost:61614'], :conn_lifetime_sec => 0.100)
    consumer.start

    DYNAMIC_URL.should == consumer.get_dynamic_fetch_url('localhost:61613')
  end

  it "check get dynamic fetch url host param array." do
    consumer = start_test_consumer(['localhost:61613', 'localhost:61614'], :conn_lifetime_sec => 0.100)
    consumer.start

    DYNAMIC_URL.should == consumer.get_dynamic_fetch_url(['localhost:61613'])
  end

  it "check fetch_serverlist url success." do
    consumer = start_test_consumer(['localhost:61613', 'localhost:61614'], :enable_dynamic_serverlist_fetch => 'true', :dynamic_serverlist_fetch_url_override => 'test')
    consumer.should_receive(:fetch_uri).with("test").at_least(1).and_return('localhost:61612, localhost:61614')
    consumer.should_receive(:start_servers).with(['localhost:61612', 'localhost:61614']);
    consumer.start
    consumer.fetch_serverlist.should =~ ['localhost:61612', 'localhost:61614']
  end

  it "check fetch_serverlist url fail." do
    consumer = start_test_consumer(['localhost:61613', 'localhost:61614'], :enable_dynamic_serverlist_fetch => 'true', :dynamic_serverlist_fetch_url_override => 'test')
    consumer.should_receive(:fetch_uri).with("test").at_least(1).and_return('<fail="localhostocalhost:61614">')

     consumer.start
     consumer.fetch_serverlist.should == nil
  end

  it "check refresh timer task with dynamic fetch url with no change." do
    consumer = start_test_consumer(['localhost:61613', 'localhost:61614'], :conn_lifetime_sec => 0.100,
                                   :enable_dynamic_serverlist_fetch => 'true', :dynamic_serverlist_fetch_url_override => 'test')
    consumer.should_receive(:fetch_serverlist).at_least(1).and_return(['localhost:61613', 'localhost:61614'])
    consumer.should_receive(:stop_servers) do |array|
      array.should =~ ['localhost:61613', 'localhost:61614']
    end.at_least(1)

    consumer.servers_running = {'localhost:61613' => 'test', 'localhost:61614' => 'test', }
    consumer.start

    sleep(0.150)
    consumer.stop
  end

  it "check refresh timer task with dynamic fetch url return with added servers." do
    consumer = start_test_consumer(['localhost:61613'], :conn_lifetime_sec => 0.100, :enable_dynamic_serverlist_fetch => 'true', :dynamic_serverlist_fetch_url_override => 'test')
    consumer.should_receive(:fetch_serverlist).at_least(1).and_return(['localhost:61613', 'localhost:61614'])        
    consumer.should_receive(:start_servers).with(['localhost:61614']).at_least(1)

    consumer.servers_running = {'localhost:61613' => 'test'}
    consumer.start

    sleep(0.150)
    consumer.stop
  end

# TODO (bbansal): Disabling dynamic server delete for now, seems like an overkill.
#                 There are chances where any user error can be disastorous.
#  it "check refresh timer task with dynamic fetch url return with deleted servers." do
#    consumer = start_test_consumer(['localhost:61613', 'localhost:61614'], :conn_lifetime_sec => 0.100, :enable_dynamic_serverlist_fetch => 'true', :dynamic_serverlist_fetch_url_override => 'test')
#    consumer.should_receive(:fetch_serverlist).at_least(1).and_return(['localhost:61613'])
#    consumer.should_receive(:stop_servers).with(['localhost:61613']).at_least(1)
#    consumer.should_receive(:start_servers).with(['localhost:61613']).at_least(1)
#    consumer.should_receive(:stop_servers).with(['localhost:61614'])
#    consumer.should_receive(:stop_servers).with(['localhost:61613', 'localhost:61614']).at_least(1)
#
#    consumer.servers_running = {'localhost:61613' => 'test', 'localhost:61614' => 'test'}
#    consumer.start
#
#    sleep(0.150)
#    consumer.stop
#  end

  it "check delete subscriptions." do
    consumer = start_test_consumer(['localhost:61613', 'localhost:61614'])
    fakeClient1 = mock(Stomp::Client)
    fakeClient2 = mock(Stomp::Client)

    fakeClient1.should_receive(:unsubscribe).with('jms.queue.testqueue')
    fakeClient2.should_receive(:unsubscribe).with('jms.queue.testqueue')

    consumer.servers_running = {'localhost:61613' => fakeClient1, 'localhost:61614' => fakeClient2}
    consumer.should_receive(:refresh_servers);
    consumer.start
    consumer.delete_subscription
  end

  it "check consumer receive blocking message available." do
    test_receive_message(['localhost:61613', 'localhost:61614'], @msg, :receive => 'blocking', :expected_message => @message)
  end

  it "check consumer receive blocking parallel thread." do
    test_receive_message(['localhost:61613', 'localhost:61614'], @msg, :receive => 'blocking', :push_message_delay => 0.050,
    :expected_message => @message)
  end

  it "check consumer receive immediate success." do
    test_receive_message(['localhost:61613', 'localhost:61614'], @msg, :receive => 'immediate', :expected_message => @message)
  end

  it "check consumer receive immediate fail." do
    test_receive_message(['localhost:61613', 'localhost:61614'], nil, :receive => 'immediate', :expected_message => nil)
  end

  it "check consumer receive immediate fail with delayed push message" do
    test_receive_message(['localhost:61613', 'localhost:61614'], @msg, :receive => 'immediate',
                         :push_message_delay => 0.050, :expected_message => nil)
  end

  it "check consumer receive immediate with client ack" do
    consumer = test_receive_message(['localhost:61613', 'localhost:61614'], @msg, :ack_type => Messagebus::ACK_TYPE_CLIENT, :receive => 'immediate',
                                     :expected_message => @message, :will_call_ack => true)
    
    consumer.ack
  end

  it "check consumer sends ack with block in safe mode" do
    consumer = test_receive_message(['localhost:61613', 'localhost:61614'], @msg, :ack_type => Messagebus::ACK_TYPE_CLIENT, :receive => 'immediate',
                                     :expected_message => @message, :will_call_ack_safe => true)
    consumer.ack true
  end

  it "check consumer receive immediate with client nack" do
    consumer = test_receive_message(['localhost:61613', 'localhost:61614'], @msg, :ack_type => Messagebus::ACK_TYPE_CLIENT, :receive => 'immediate',
                                     :expected_message => @message, :will_call_nack => true)
    
    consumer.nack
  end
  
  it "check consumer keepalive" do
    consumer = test_receive_message(['localhost:61613', 'localhost:61614'], @msg, :ack_type => Messagebus::ACK_TYPE_CLIENT, :receive => 'immediate',
                                     :expected_message => @message, :will_call_keepalive => true)
    
    consumer.keepalive
  end

  it "check consumer receive timeout fail." do
    expect {
      test_receive_message(['localhost:61613', 'localhost:61614'], nil, :receive => 'timeout', :timeout_ms => 100, :push_message_delay => 0.500,
        :expected_message => nil)
    }.to raise_error(Messagebus::MessageReceiveTimeout, /receive timeout/)

    sleep(0.200)
  end

  it "check consumer receive timeout success." do
    test_receive_message(['localhost:61613', 'localhost:61614'], @msg, :receive => 'timeout', :timeout_ms => 100,
                         :push_message_delay => 0.050, :expected_message => @message)
  end

  it "check consumer receive string message success." do
    test_receive_message(['localhost:61613', 'localhost:61614'], @msg, :receive => 'timeout', :timeout_ms => 100,
                         :push_message_delay => 0.050, :expected_message => @message)
  end
end
