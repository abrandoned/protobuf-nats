require "nats/io/client"
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

$nats = ::NATS::IO::Client.new
$nats.connect({:servers => ["nats://127.0.0.1:4222"]})

subscription_key_and_queue = "Warehouse::ShipmentService::create"

Benchmark.ips do |config|
  config.warmup = 10
  config.time = 10

  config.report("single threaded performance") do
    $nats.request(subscription_key_and_queue, $request)
  end
end
