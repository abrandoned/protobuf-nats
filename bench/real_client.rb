ENV["PB_SERVER_TYPE"] = "protobuf/nats/runner"
ENV["PB_CLIENT_TYPE"] = "protobuf/nats/client"

require "benchmark/ips"
require "./examples/warehouse/app"

Protobuf::Logging.logger = ::Logger.new(nil)

Benchmark.ips do |config|
  config.warmup = 10
  config.time = 10

  config.report("single threaded performance") do
    begin
      req = Warehouse::Shipment.new(:guid => SecureRandom.uuid)
      Warehouse::ShipmentService.client.create(req)
    rescue => error
      puts error
    end
  end
end
