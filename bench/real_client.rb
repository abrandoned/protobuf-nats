ENV["PB_CLIENT_TYPE"] = "protobuf/nats/client"
ENV["PB_SERVER_TYPE"] = "protobuf/nats/runner"

require "benchmark/ips"
require "./examples/warehouse/app"

payload = ::Warehouse::Shipment.new(:guid => ::SecureRandom.uuid, :address => "123 yolo st")
nats = ::NATS::IO::Client.new
nats.connect({:servers => ["nats://127.0.0.1:4222"]})

Protobuf::Logging.logger = ::Logger.new(nil)

Benchmark.ips do |config|
  config.warmup = 10
  config.time = 10

  config.report("single threaded performance") do
    req = Warehouse::Shipment.new(:guid => SecureRandom.uuid)
    Warehouse::ShipmentService.client.create(req)
  end
end
