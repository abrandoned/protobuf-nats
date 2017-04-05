# ENV["PB_CLIENT_TYPE"] = "zmq"
# ENV["PB_CLIENT_TYPE"] = "zmq"
ENV["PB_SERVER_TYPE"] = "protobuf/nats/runner"
ENV["PB_CLIENT_TYPE"] = "protobuf/nats/client"

require "benchmark/ips"
require "./examples/warehouse/app"
# require "ruby-prof"
#require 'ruby-prof-flamegraph'

Protobuf::Logging.logger = ::Logger.new(nil)

# 100.times do
#   req = Warehouse::Shipment.new(:guid => SecureRandom.uuid)
#   Warehouse::ShipmentService.client.create(req)
# end
#result = RubyProf.profile do

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

#end

# file = File.open("/tmp/flame-prof-nats-2.prof", "w+")
# printer = RubyProf::FlameGraphPrinter.new(result)
# printer.print(file, {})
# file.close
