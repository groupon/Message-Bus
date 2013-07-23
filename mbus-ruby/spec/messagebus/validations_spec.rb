require File.join(File.dirname(__FILE__), '..', 'spec_helper')

describe Messagebus::Validations do
  describe "#validate_destination_config" do
    before do
      @object = Object.new
      @object.extend Messagebus::Validations
    end

    it "requires a destination name" do
      lambda {
        @object.validate_destination_config(nil)
      }.should raise_error(Messagebus::InvalidDestination)
    end

    it "requires a topic to include a subscription_id" do
      lambda {
        @object.validate_destination_config("jms.topic.testTopic1", true)
      }.should raise_error(Messagebus::InvalidDestination)
    end

    it "validates when a topic includes a subscription_id" do
      lambda {
        @object.validate_destination_config("jms.topic.testTopic1", true, :subscription_id => "test1")
      }.should_not raise_error(Messagebus::InvalidDestination)
    end
  end

  describe "#validate_connection_config" do
    before do
      @object = Object.new
      @object.extend Messagebus::Validations
    end

    it "does not recognize an acknowledgement type other than AUTO_CLIENT AND CLIENT" do
      lambda {
        @object.validate_connection_config("localhost:61613", :ack_type => "")
      }.should raise_error(Messagebus::InvalidAcknowledgementType)
    end

    it "requires host parameters" do
      lambda {
        @object.validate_connection_config(nil)
      }.should raise_error(Messagebus::InvalidHost)
    end

    it "requires an array of host parameters" do
      lambda {
        @object.validate_connection_config("localhost:61613")
      }.should raise_error(Messagebus::InvalidHost)
    end

    it "fails if the host is missing from the host parameters" do
      lambda {
        @object.validate_connection_config([":61613"])
      }.should raise_error(Messagebus::InvalidHost)
    end

    it "fails if the port is missing from the host parameters" do
      lambda {
        @object.validate_connection_config(["localhost"])
      }.should raise_error(Messagebus::InvalidHost)
    end

    it "should not fail if host contains a zero" do
      lambda {
        @object.validate_connection_config(["somehost-001:61613"])
      }.should_not raise_error(Messagebus::InvalidHost)
    end
  end
end
