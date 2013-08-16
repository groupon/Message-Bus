# -*- encoding: utf-8 -*-
require "#{File.dirname(__FILE__)}/lib/messagebus/version"

Gem::Specification.new do |gem|
  gem.authors       = ["Pradeep Jawahar", "Lin Zhao", "Erik Weathers"]
  gem.email         = ["pjawahar@groupon.com", "lin@groupon.com", "eweathers@groupon.com"]
  gem.description   = %q{Messagebus integration gem}
  gem.summary       = %q{Messagebus Client}
  gem.homepage      = "https://github.com/groupon/Message-Bus"

  gem.executables   = %w(messagebus_swarm)
  gem.files         = Dir['bin/*'] + Dir["lib/**/*.rb"] + Dir["vendor/**/*"] + %w(README.mediawiki Rakefile messagebus.gemspec)
  gem.test_files    = Dir["spec*/**/*.rb"]
  gem.name          = "messagebus"
  gem.require_paths = ["vendor/gems", "lib"]
  gem.version       = Messagebus::VERSION
  gem.license       = 'BSD'

  gem.required_rubygems_version = ">= 1.3.6"

  if RUBY_VERSION < "1.9"
    # json is built into ruby 1.9
    gem.add_dependency "json"
  end
  gem.add_dependency "thrift", "0.9.0"

  gem.add_development_dependency "rspec"
  gem.add_development_dependency "simplecov"
end
