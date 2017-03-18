require "protobuf/rpc/server"
require "concurrent"

module Protobuf
  module Nats
    class Server
      include ::Protobuf::Rpc::Server
      include ::Protobuf::Logging

      attr_reader :nats, :thread_pool, :subscriptions

      def self.service_klasses
        ::ObjectSpace.each_object(::Class).select { |klass| klass < ::Protobuf::Rpc::Service }
      end

      def initialize(options)
        @options = options
        @running = true
        @stopped = false

        @nats = ::NATS::IO::Client.new
        @nats.connect(::Protobuf::Nats.connection_options)
        @thread_pool = ::Concurrent::FixedThreadPool.new(options[:threads])

        @subscriptions = []
      end


      def service_klasses
        self.class.service_klasses
      end

      def execute_request_promise(request_data, reply_id)
        ::Concurrent::Promise.new(:executor => thread_pool).then do
          response_data = handle_request(request_data)
          nats.publish(reply_id, response_data)
        end.on_error do |error|
          logger.error error
          if error.respond_to?(:backtrace) && error.backtrace.is_a?(::Array)
            logger.error error.backtrace.join("\n")
          end
        end.execute
      end

      def subscribe_to_service(service_klass)
        service_klass.rpcs.each do |service_name, _|
          subscription_key_and_queue = "#{service_klass}::#{service_name}"

          subscriptions << nats.subscribe(subscription_key_and_queue, :queue => subscription_key_and_queue) do |request_data, reply_id, _subject|
            execute_request_promise(request_data, reply_id)
          end
        end
      end

      def run
        service_klasses.each do |service_klass|
          subscribe_to_service(service_klass)
        end

        yield if block_given?

        loop do
          break unless @running
          sleep 1
        end

        subscriptions.each do |subscription_id|
          nats.unsubscribe(subscription_id)
        end

        thread_pool.shutdown
        thread_pool.wait_for_termination
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
