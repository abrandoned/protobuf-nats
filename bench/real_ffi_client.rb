require "benchmark/ips"
require "protobuf"
require 'protobuf/message'
require 'protobuf/rpc/service'
require "protobuf/nats"

module Warehouse
  class Shipment < ::Protobuf::Message
    optional :string, :guid, 1
    optional :string, :address, 2
    optional :double, :price, 3
    optional :string, :package_guid, 4
  end

  class ShipmentService < ::Protobuf::Rpc::Service
    rpc :create, ::Warehouse::Shipment, ::Warehouse::Shipment
  end
end

Protobuf::Logging.logger = ::Logger.new(nil)

Benchmark.ips do |config|
  config.warmup = 10
  config.time = 10

  config.report("single threaded performance") do
    req = Warehouse::Shipment.new(:guid => SecureRandom.uuid)
    Warehouse::ShipmentService.client.create(req)
  end
end
