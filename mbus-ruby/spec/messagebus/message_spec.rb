# encoding: UTF-8
require File.join(File.dirname(__FILE__), '..', 'spec_helper')

describe Messagebus::Message do
  describe "when checking non-ascii payload" do
    describe "when checking conversions" do
      before(:each) do
        @msg_string_data1 = 'am – 5:00 pm,'
        @gb2312_string = "\xC4\xE3\xBA\xC3"
        @msg_string_data2 = 'ASCII String'
        @msg_json_object = {:a=> '1', :b => {:x => 'Thorbjørn am – 5:00 pm,'}}
        @msg_binary_data = "\xE5\xA5\xBD"
      end
      
      it "check non-ascii string" do
        message = Messagebus::Message.create(@msg_string_data1)
        message2 = Messagebus::Message.get_message_from_thrift_binary(message.to_thrift_binary)
        @msg_string_data1.bytes.to_a.should == message2.payload.bytes.to_a
      end
      
      it "check ASCII string" do
        if RUBY_VERSION.to_f >= 1.9
          @msg_string_data2.force_encoding("ASCII")
        end
        message = Messagebus::Message.create(@msg_string_data2)
        message2 = Messagebus::Message.get_message_from_thrift_binary(message.to_thrift_binary)
        @msg_string_data2.bytes.to_a.should == message2.payload.bytes.to_a
        
      end

      it "check gb2312 string" do
        if RUBY_VERSION.to_f >= 1.9
          @gb2312_string.force_encoding("GB2312")
        end
        message = Messagebus::Message.create(@gb2312_string)
        message2 = Messagebus::Message.get_message_from_thrift_binary(message.to_thrift_binary)
        @gb2312_string.bytes.to_a.should == message2.payload.bytes.to_a
      end

      it "check json thrift conversions" do
        message = Messagebus::Message.create(@msg_json_object)
        message2 = Messagebus::Message.get_message_from_thrift_binary(message.to_thrift_binary)
        json_string = @msg_json_object.to_json
        json_string.bytes.to_a.should == message2.payload.bytes.to_a
      end

      it "check binary thrift equality" do
        message = Messagebus::Message.create(@msg_binary_data, nil, true)
        message2 = Messagebus::Message.get_message_from_thrift_binary(message.to_thrift_binary)
        @msg_binary_data.bytes.to_a == message2.payload.bytes.to_a
        
      end       

      it "check message id are unique" do
        message_ids = Set.new
        (1..100).each do |i|
          message = Messagebus::Message.create(@msg_binary_data)
          message_ids.include?(message.message_id).should == false
          message_ids.add(message.message_id)
        end  
      end
    end
  end
  describe "#create" do
    describe "with a hash" do
      it "returns a message of json type" do
        Messagebus::Message.create({:benjamin => :franklins}).
          raw_message.payload.should be_json
      end
    end

    describe "with an object that responds to to_json" do
      it "returns a message of json type" do
        Messagebus::Message.create(Object.new).
          raw_message.payload.should be_json
      end
    end

    describe "with a binary string and binary arg" do
      it "returns a message of binary type" do
        Messagebus::Message.create("\xE5", nil, true).
          raw_message.payload.should be_binary
      end
    end

    describe "with a string" do
      it "returns a message of string type" do
        Messagebus::Message.create("benjamins").
          raw_message.payload.should be_string
      end
    end
  end
end
