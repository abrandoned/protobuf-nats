require "protobuf/rpc/server"

module Protobuf
  module Nats
    class Server
      include ::Protobuf::Rpc::Server

      def self.service_klasses
        ::ObjectSpace.each_object(::Class).select { |klass| klass < ::Protobuf::Rpc::Service }
      end

      def initialize(options)
        @options = options
        @running = true
        @stopped = false
      end

      def service_klasses
        self.class.service_klasses
      end

      def subscribe_to_service(service_klass)
        service_klass.rpcs.each do |service_name, _|
          subscription_key_and_queue = "#{service_klass}::#{service_name}"

          ::Protobuf::Nats::Config.server_nats_pool.each do |nats|
            nats.subscribe(subscription_key_and_queue, :queue => subscription_key_and_queue) do |request_data, reply_id, _subject|
              response_data = handle_request(request_data)
              nats.publish(reply_id, response_data)
            end
          end
        end
      end

      def run
        # Ensure the server pool is connected so we can subscribe.
        pool_size = 5
        Protobuf::Nats.start_server_pool(pool_size)
        pool_size.times do
          service_klasses.each do |service_klass|
            subscribe_to_service(service_klass)
          end
        end

        yield if block_given?

        loop do
          break unless @running
          sleep 1
        end

        # TODO: Unsusbscribe before shutting down?

      ensure
        @stopped = true
      end

      def running?
        @stopped
      end

      def stop
        @running = false
      end
    end
  end
end
