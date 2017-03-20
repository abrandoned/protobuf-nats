require "ffi/nats/core"
require "thread"
require "benchmark/ips"
require "protobuf"
require 'protobuf/message'
require 'protobuf/rpc/service'

module Warehouse
  class Shipment < ::Protobuf::Message
    optional :string, :guid, 1
    optional :string, :address, 2
    optional :double, :price, 3
    optional :string, :package_guid, 4
  end
end

payload = ::Warehouse::Shipment.new(:guid => ::SecureRandom.uuid, :address => "123 yolo st")
$request = ::Protobuf::Socketrpc::Request.new(:service_name => "Warehouse",
                                               :method_name => "create",
                                               :request_proto => payload.encode).encode

options_pointer = FFI::MemoryPointer.new :pointer
connection_pointer = FFI::MemoryPointer.new :pointer

FFI::Nats::Core.natsOptions_Create(options_pointer)
options_pointer = options_pointer.get_pointer(0)
FFI::Nats::Core.natsOptions_SetURL(options_pointer, "nats://localhost:4222")

FFI::Nats::Core.natsConnection_Connect(connection_pointer, options_pointer)
connection_pointer = connection_pointer.get_pointer(0)

subscription_key_and_queue = "Warehouse::ShipmentService::create"

Benchmark.ips do |config|
  config.warmup = 10
  config.time = 10

  config.report("single threaded performance") do
    FFI::MemoryPointer.new(:pointer) do |message_pointer|
      FFI::Nats::Core.natsConnection_RequestString(message_pointer, connection_pointer, subscription_key_and_queue, $request, 1000)
      FFI::Nats::Core.natsMsg_Destroy(message_pointer.get_pointer(0))
    end
  end
end
