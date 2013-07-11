module Messagebus
  module Swarm
    def self.logger=(logger)
      @logger = logger
    end

    def self.logger
      @logger
    end

    def self.default_configuration=(default_configuration)
      @default_configuration = default_configuration
    end

    def self.default_configuration
      @default_configuration
    end
  end
end