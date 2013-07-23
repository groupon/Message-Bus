class SimpleWorker
    def self.perform(message_payload)
      # success
      puts message_payload.inspect
    end
  end
