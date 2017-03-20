require "protobuf"
require 'protobuf/message'
require 'protobuf/rpc/service'
require "ffi/nats/core"
require "concurrent"

module Warehouse
  class Shipment < ::Protobuf::Message
    optional :string, :guid, 1
    optional :string, :address, 2
    optional :double, :price, 3
    optional :string, :package_guid, 4
  end
end

$thread_pool = ::Concurrent::FixedThreadPool.new(20)
payload = ::Warehouse::Shipment.new(:guid => ::SecureRandom.uuid, :address => "123 yolo st")
$response = ::Protobuf::Socketrpc::Response.new(:response_proto => payload.encode).encode
subscription_key_and_queue = "Warehouse::ShipmentService::create"

subscription = FFI::MemoryPointer.new :pointer
opts_pointer = FFI::MemoryPointer.new :pointer
conn_t = FFI::MemoryPointer.new :pointer

FFI::Nats::Core.natsOptions_Create(opts_pointer)
opts_pointer = opts_pointer.get_pointer(0)
FFI::Nats::Core.natsOptions_SetURL(opts_pointer, "nats://localhost:4222")
FFI::Nats::Core.natsOptions_UseGlobalMessageDelivery(opts_pointer, true)

FFI::Nats::Core.natsConnection_Connect(conn_t, opts_pointer)
conn_t = conn_t.get_pointer(0)

callback = FFI::Function.new(:void, [:pointer, :pointer, :pointer, :pointer], :blocking => true) do |conn, sub, msg, closure|
  ::Concurrent::Promise.new(:executor => $thread_pool).then do
    reply_to, _ = FFI::Nats::Core.natsMsg_GetReply(msg)
    FFI::Nats::Core.natsConnection_PublishString(conn, reply_to, $response)
    FFI::Nats::Core.natsConnection_Flush(conn)
    FFI::Nats::Core.natsMsg_Destroy(msg)
  end.on_error do |error|
    logger.error error
    if error.respond_to?(:backtrace) && error.backtrace.is_a?(::Array)
      logger.error error.backtrace.join("\n")
    end
  end.execute
end

FFI::Nats::Core.natsConnection_QueueSubscribe(subscription, conn_t, subscription_key_and_queue, subscription_key_and_queue, callback, nil)
FFI::Nats::Core.natsConnection_Flush(conn_t)

puts "Server started!"

loop do
  sleep 1
end
