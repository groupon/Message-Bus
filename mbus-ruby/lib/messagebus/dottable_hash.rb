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
