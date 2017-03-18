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
        attr_accessor :client_nats_pool
      end
    end

    def self.connection_options
      {:servers => ["nats://127.0.0.1:4222"]}
    end

    def self.start_client_pool
      Config.client_nats_pool = ConnectionPool.new(size: 3, timeout: 2) do
        client = ::NATS::IO::Client.new
        client.connect(connection_options)
        client
      end

      true
    end

    def self.ensure_client_pools_started
      @ensure_client_pools_started ||= start_client_pool
    end
  end
end
