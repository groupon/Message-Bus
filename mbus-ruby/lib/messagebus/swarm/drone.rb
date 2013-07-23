# Copyright (c) 2012, Groupon, Inc.
# All rights reserved.
# 
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
# 
# Redistributions of source code must retain the above copyright notice,
# this list of conditions and the following disclaimer.
# 
# Redistributions in binary form must reproduce the above copyright
# notice, this list of conditions and the following disclaimer in the
# documentation and/or other materials provided with the distribution.
# 
# Neither the name of GROUPON nor the names of its contributors may be
# used to endorse or promote products derived from this software without
# specific prior written permission.
# 
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
# IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
# TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
# PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
# HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
# SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
# TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
# PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
# LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
# NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

require 'json'

module Messagebus
  module Swarm
    ##
    # This is a composition of a consumer and a separate message processor.
    # It allows you to use Plain-Old-Ruby-Objects to do message processing.
    # See #initialize for how the messages are delegated.
    class Drone
      # Raise this error when you want to abort processing a message and not
      # acknowledge it.
      class AbortProcessing < StandardError; end

      INITIALIZING = "initializing"
      RECEIVING    = "receiving"
      PROCESSING   = "processing"
      COMPLETED    = "completed"

      STOP_PROCESSING_MESSAGE = "stop processing message"

      attr_reader :state, :id

      ##
      # Expected options:
      # * :ack_on_error (default false): whether to ack the message when an error was raised. Aliased to auto_acknowledge for backwards compatibility
      # * :consumer (required): a consumer object for that topic
      # * :destination_name (required): the message bus queue/topic name
      # * :id: an id for this drone (just used for debugging)
      # * :logger (required): the logger to publish messages to
      # * :worker_class (required): the actual worker that will be used to do the processing
      #
      # As messages come down, they will be passed to the worker's perform or
      # perform_on_destination method. The methods will be called in the
      # following priority (if they exist):
      # * perform_on_destination(message_payload, destination_message_came_from)
      # * perform(message_payload)
      #
      # A message is processed by:
      #   message = receive_message
      #   begin
      #     worker.perform(message)
      #     ack(message)
      #   rescue AbortProcessing
      #     # raise this error if you want to say "Don't ack me"
      #   rescue StandardError
      #     ack(message) if ack_on_error
      #   end
      def initialize(options)
        @auto_acknowledge = options.fetch(:ack_on_error, options[:auto_acknowledge])
        @consumer, @destination_name, @worker_class, @id, @logger = options.values_at(:consumer, :destination_name, :worker_class, :id, :logger)
        @state = INITIALIZING
        @logger.debug { "initializing a drone drone_id=#{@id}" }
      end

      ##
      # This is the main loop for the drone's work. This will not return until
      # the drone is stopped via #stop.
      #
      # If the consumer hasn't been started yet, this method will start it. It
      # also will auto close the consumer in that case.
      def processing_loop
        @processing = true

        auto_started_consumer = false
        begin
          if !@consumer.started?
            @logger.debug "auto starting the consumer drone_id=#{@id}"
            @consumer.start
            auto_started_consumer = true
          end

          while @processing
            message = nil
            begin
              @logger.debug "waiting for a message"

              @state = RECEIVING
              message = @consumer.receive
              # intentional === here, this is used as a signal, so we can use object equality
              # to check if we received the signal
              if message === STOP_PROCESSING_MESSAGE
                @logger.info "received a stop message, exiting drone_id=#{@id}, message=#{message.inspect}"
                @state = COMPLETED
                next
              end

              @logger.info "received message drone_id=#{@id}, message_id=#{message.message_id}"

              @state = PROCESSING
              @logger.info "processing message drone_id=#{@id}, message_id=#{message.message_id}, worker=#{@worker_class.name}"

              raw_message = extract_underlying_message_body(message)
              @logger.debug { "drone_id=#{@id} message_id=#{message.message_id}, message=#{raw_message.inspect}" }

              worker_perform(@logger, @destination_name, @worker_class, @consumer, @auto_acknowledge, message, raw_message)

              @state = COMPLETED
            rescue => except
              @logger.warn "exception processing message drone_id=#{@id}, message_id=#{message && message.message_id}, exception=\"#{except.message}\", exception_backtrace=#{except.backtrace.join("|")}"
            end
          end
        ensure
          if auto_started_consumer
            @logger.debug "auto stopping the consumer drone_id=#{@id}"
            @consumer.stop
          end
        end

        @logger.info("gracefully exited drone_id=#{@id}")
      end

      ##
      # Stops this drone from processing any additional jobs.
      # This will not abort any in progress jobs.
      def stop
        @logger.info("received stop message, current_state=#{@state}, processing=#{@processing}, drone_id=#{@id}")
        return if !@processing

        @processing = false
        @consumer.insert_sentinel_value(STOP_PROCESSING_MESSAGE)
      end

      private
      def blocking_on_receive?(state)
        state == RECEIVING
      end

      def worker_perform(logger, destination_name, worker_class, consumer, auto_acknowledge, message, raw_message)
        begin
          if worker_class.respond_to?("perform_on_destination")
            worker_class.perform_on_destination(raw_message, destination_name)
          else
            worker_class.perform(raw_message)
          end

          # acknowledge unless exception is thrown (auto perform == true)
          logger.debug "processing complete, acknowledging message, drone_id=#{@id}, message_id=#{message.message_id}"
          consumer.ack

        # This is just a clean error people can throw to trigger an abort
        rescue AbortProcessing => e
          logger.info "processing aborted drone_id=#{@id}, message_id=#{message.message_id}"
        rescue => worker_exception
          logger.warn "processing failed drone_id=#{@id}, message_id=#{message.message_id} exception=\"#{worker_exception.message}\", exception_backtrace=#{worker_exception.backtrace.join("|")}"

          if auto_acknowledge
            logger.info "despite failure, auto acknowledging message, drone_id=#{@id}, message_id=#{message.message_id}"
            consumer.ack
          end
        end
      end

      def extract_underlying_message_body(message)
        payload = message.raw_message.payload
        if payload.json?
          JSON.parse(payload.stringPayload)
        elsif payload.binary?
          payload.binaryPayload
        elsif payload.string?
          payload.stringPayload
        end
      end
    end
  end
end
