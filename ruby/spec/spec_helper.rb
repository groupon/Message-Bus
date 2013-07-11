require "rspec"

$:.unshift(File.join(File.dirname(__FILE__), "..", "lib"))
$:.unshift(File.join(File.dirname(__FILE__), "..", "vendor/gems"))

require "stomp"
require 'messagebus'
require 'messagebus/swarm/controller'
require 'thrift'
require 'json'
