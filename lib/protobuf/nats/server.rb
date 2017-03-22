require "active_support/core_ext/class/subclasses"
require "concurrent"
require "protobuf/rpc/server"
require "protobuf/rpc/service"

module Protobuf
  module Nats
    class Server
      include ::Protobuf::Rpc::Server
      include ::Protobuf::Logging

      attr_reader :nats, :thread_pool, :subscriptions

      def initialize(options)
        @options = options
        @running = true
        @stopped = false

        @nats = ::NATS::IO::Client.new
        @nats.connect(::Protobuf::Nats::Config.connection_options)
        @thread_pool = ::Concurrent::FixedThreadPool.new(options[:threads])

        @subscriptions = []
      end

      def service_klasses
        ::Protobuf::Rpc::Service.implemented_services.map(&:safe_constantize)
      end

      def execute_request_promise(request_data, reply_id)
        ::Concurrent::Promise.new(:executor => thread_pool).then do
          # Publish an ACK.
          nats.publish(reply_id, ::Protobuf::Nats::Messages::ACK)
          # Process request.
          response_data = handle_request(request_data)
          # Publish response.
          nats.publish(reply_id, response_data)
        end.on_error do |error|
          logger.error error
          if error.respond_to?(:backtrace) && error.backtrace.is_a?(::Array)
            logger.error error.backtrace.join("\n")
          end
        end.execute
      end

      def subscribe_to_services
        logger.info "Creating subscriptions:"

        service_klasses.each do |service_klass|
          service_klass.rpcs.each do |service_name, _|
            # Skip services that are not implemented.
            next unless service_klass.method_defined? service_name

            subscription_key_and_queue = "#{service_klass}::#{service_name}"
            logger.info "  - #{subscription_key_and_queue}"

            subscriptions << nats.subscribe(subscription_key_and_queue, :queue => subscription_key_and_queue) do |request_data, reply_id, _subject|
              execute_request_promise(request_data, reply_id)
            end
          end
        end
      end

      def run
        nats.on_reconnect do
          logger.warn "Reconnected to NATS server!"
          subscribe_to_services
        end

        nats.on_disconnect do
          logger.warn "Disconnected from NATS server!"
        end

        subscribe_to_services

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
