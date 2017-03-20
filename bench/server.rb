require "nats/io/client"
require "concurrent"
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

$thread_pool = ::Concurrent::FixedThreadPool.new(20)
payload = ::Warehouse::Shipment.new(:guid => ::SecureRandom.uuid, :address => "123 yolo st")
$response = ::Protobuf::Socketrpc::Response.new(:response_proto => payload.encode).encode

$nats = ::NATS::IO::Client.new
$nats.connect({:servers => ["nats://127.0.0.1:4222"]})

subscription_key_and_queue = "Warehouse::ShipmentService::create"
$nats.subscribe(subscription_key_and_queue, :queue => subscription_key_and_queue) do |request_data, reply_id, _subject|
  ::Concurrent::Promise.new(:executor => $thread_pool).then do
    $nats.publish(reply_id, $response)
  end.on_error do |error|
    logger.error error
    if error.respond_to?(:backtrace) && error.backtrace.is_a?(::Array)
      logger.error error.backtrace.join("\n")
    end
  end.execute
end

puts "Server started!"

loop do
  sleep 1
end
