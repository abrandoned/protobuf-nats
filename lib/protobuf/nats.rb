require "protobuf/nats/version"

require "protobuf"
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
        attr_accessor :server_nats_pool
      end
    end

    def self.connection_options
      {:servers => ["nats://127.0.0.1:4222"]}
    end

    def self.start_client_pool
      Config.client_nats_pool = ConnectionPool.new(size: 50, timeout: 2) do
        client = ::NATS::IO::Client.new
        client.connect(connection_options)
        client
      end

      true
    end

    def self.start_server_pool(size = 5)
      Config.server_nats_pool = size.times.map do
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
