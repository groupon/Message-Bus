module Messagebus
  class DottableHash < Hash

    def initialize(plain_old_hash={})
      merge!(plain_old_hash)
    end

    def respond_to?(method)
      true
    end

    def method_missing(method, *arguments, &block)
      key = method.to_s
      if key.match(/\=$/)
        self[key.chop] = arguments.first
      elsif self.has_key?(key)
        self[key]
      end
    end

    def [](key)
      super(key.to_s)
    end

    def []=(key, value)
      super(key.to_s, deep_stringify(value))
    end

    def merge!(hash)
      super(stringify_keys(hash))
    end

    def replace(hash)
      super(stringify_keys(hash))
    end

    def delete(key)
      super(key.to_s)
    end

    def has_key?(key)
      super(key.to_s)
    end

    def fetch(key)
      super(key.to_s)
    end

    def assoc(key)
      super(key.to_s)
    end

    def values_at(*args)
      super(*args.collect(&:to_s))
    end

    alias :store :[]=
    alias :update :merge!
    alias :merge :merge!
    alias :key? :has_key?
    alias :include? :has_key?

    private

    def stringify_keys(hash)
      hash.inject({}) do |acc, (key, value)|
        acc[key.to_s] = deep_stringify(hash[key])
        acc
      end
    end

    def deep_stringify(element)
      case element
      when Array
        element.collect {|value| deep_stringify(value)}
      when Hash
        self.class.new(stringify_keys(element))
      else
        element
      end
    end
  end
end