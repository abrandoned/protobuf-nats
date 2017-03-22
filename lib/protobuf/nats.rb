require "protobuf/nats/version"

require "protobuf"
# We don't need this, but the CLI attempts to terminate.
require "protobuf/rpc/service_directory"

require "connection_pool"

require "nats/io/client"
require "protobuf/nats/client"
require "protobuf/nats/server"
require "protobuf/nats/runner"

module Protobuf
  module Nats
    class Config
      class << self
        attr_accessor :client_nats_connection
      end
    end

    def self.connection_options
      {:servers => ["nats://127.0.0.1:4222"]}
    end

    def self.start_client_nats_connection
      Config.client_nats_connection = ::NATS::IO::Client.new
      Config.client_nats_connection.connect(connection_options)
      true
    end

    def self.ensure_client_nats_connection_started
      @ensure_client_nats_connection_started ||= start_client_nats_connection
    end
  end
end
