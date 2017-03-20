require "protobuf/nats/version"

require "protobuf"
# We don't need this, but the CLI attempts to terminate.
require "protobuf/rpc/service_directory"

require "connection_pool"

$ffi_enabled = ENV.key?("FFI")
if $ffi_enabled
  require "ffi/nats/core"
  require "protobuf/nats/ffi_client"
  require "protobuf/nats/ffi_server"
else
  require "nats/io/client"
  require "protobuf/nats/client"
  require "protobuf/nats/server"
end

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

    def self.ffi?
      $ffi_enabled
    end

    def self.start_client_nats_connection
      if ffi?
        options_pointer = FFI::MemoryPointer.new :pointer
        connection_pointer = FFI::MemoryPointer.new :pointer

        FFI::Nats::Core.natsOptions_Create(options_pointer)
        options_pointer = options_pointer.get_pointer(0)
        FFI::Nats::Core.natsOptions_SetURL(options_pointer, "nats://localhost:4222")

        FFI::Nats::Core.natsConnection_Connect(connection_pointer, options_pointer)
        Config.client_nats_connection = connection_pointer = connection_pointer.get_pointer(0)

      else
        Config.client_nats_connection = ::NATS::IO::Client.new
        Config.client_nats_connection.connect(connection_options)
      end
      true
    end

    def self.ensure_client_nats_connection_started
      @ensure_client_nats_connection_started ||= start_client_nats_connection
    end
  end
end
