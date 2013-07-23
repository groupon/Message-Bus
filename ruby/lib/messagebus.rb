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

$:.unshift(File.join(File.dirname(__FILE__), "..", "vendor/gems"))

require "messagebus/version"
require 'messagebus/logger'
require 'messagebus/message'
require "messagebus/messagebus_types"
require 'messagebus/custom_errors'
require 'messagebus/error_status'
require 'messagebus/validations'
require 'messagebus/connection'
require 'messagebus/producer'
require 'messagebus/consumer'
require 'messagebus/cluster_map'
require 'messagebus/client'

require 'thrift'
require 'base64'
require 'stomp'

module Messagebus
  # These are deprecated and not used anymore
  DEST_TYPE_QUEUE = 'queue'
  DEST_TYPE_TOPIC = 'topic'

  ACK_TYPE_AUTO_CLIENT = 'autoClient'
  ACK_TYPE_CLIENT = 'client'

  LOG_DEFAULT_FILE = '/tmp/messagebus_client.log'

  SERVER_REGEX = /^[a-zA-Z0-9\_.-]+:[0-9]+$/
end

