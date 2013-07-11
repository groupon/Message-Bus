require "json"
require "thrift"
require "digest/md5"

module Messagebus
  # Container class for frames, misnamed technically
  class Message
    attr_reader :raw_message 

    @@serializer = ::Thrift::Serializer.new
    @@deserializer = ::Thrift::Deserializer.new
    @@serializer_lock = Mutex.new
    @@deserializer_lock = Mutex.new

    class << self
      def create(payload, properties = nil, binary = false)
        Messagebus::Message.create_message(define_thrift(payload, binary), properties)
      end

      # Creates message from base64 encoded thrift bytes.
      # We use Stomp protocol for server communication which internally
      # uses string (utf-8). Base64 encoding is needed to avoid corner
      # cases with weird bytes etc.
      def get_message_from_thrift_binary(body)
        binary_string = Base64.decode64(body)
        rmessage = nil
        @@deserializer_lock.synchronize do
          rmessage = @@deserializer.deserialize(Messagebus::Thrift::MessageInternal.new, binary_string)
        end
        Messagebus::Message.create_message_from_message_internal(rmessage)
      end

      def define_thrift(payload, binary = false)
        options = {}
        
        if binary        
	  if RUBY_VERSION.to_f >= 1.9 
      	      payload.force_encoding('UTF-8')
      	  end
          options.merge!({
            :messageFormat => Messagebus::Thrift::MessagePayloadType::BINARY,
            :binaryPayload => payload
          })
        elsif payload.is_a?(Hash) || (payload.respond_to?(:to_json) && !payload.is_a?(String))
          options.merge!({
            :messageFormat => Messagebus::Thrift::MessagePayloadType::JSON,
            :stringPayload => payload.to_json
          })
        elsif payload.is_a?(String)
          #Only UTF-8 is supported by thrift. Should warn to use binary if not ascii or utf-8 but there's no logger
          #I believe all users use utf8 or ascii.
          if RUBY_VERSION.to_f >= 1.9 
             payload.force_encoding('UTF-8')
          end
          options.merge!({
            :messageFormat => Messagebus::Thrift::MessagePayloadType::STRING,
            :stringPayload => payload
          })
        else
          # TODO: Create specific error class
          raise "Type not supported"
        end

        Messagebus::Thrift::MessagePayload.new(options)
      end
 
      # TODO: Why use this in utils when
      # trying to follow a factory pattern?
      def create_message_from_message_internal(raw_message)
        Message.new(raw_message)
      end
    end

    def message_id
      @raw_message.messageId
    end          
    
    def message_properties
      @raw_message.properties
    end 
    
    def message_properties=(hash)
      @raw_message.properties = hash
    end 
    
    def payload_type
      @raw_message.payload.messageFormat
    end
    
    def payload
      payload = @raw_message.payload

      if payload.binary?
        @raw_message.payload.binaryPayload
      elsif payload.json? || payload.string?
        @raw_message.payload.stringPayload
      else
        raise "Payload is not an understandable type: #{payload.messageFormat}"
      end
    end

    def to_thrift_binary
      binary_string = nil
      @@serializer_lock.synchronize do
        binary_string = @@serializer.serialize(@raw_message)
      end
      Base64.encode64(binary_string);
    end
 
    private
    
    def initialize(raw_message)
      @raw_message = raw_message
    end

        
    class << self
      # Returns the payload as a string salted with current milliseconds since epoch.
      def get_salted_payload(payload)
        t = Time.now
        data = payload.binary? ? payload.binaryPayload : payload.stringPayload
        data += t.to_i.to_s
        data += t.tv_usec.to_s 
        data
      end
      
      def create_message(payload, properties = nil)
        raw_message = Messagebus::Thrift::MessageInternal.new
        raw_message.messageId = Digest::MD5.hexdigest(get_salted_payload(payload))
        raw_message.payload = payload
        raw_message.properties = properties if properties

        Message.new(raw_message)
      end
    end
  end
end

