require "active_support/core_ext/class/subclasses"
require "protobuf/rpc/server"
require "protobuf/rpc/service"
require "protobuf/nats/thread_pool"

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

        @nats = @options[:client] || ::Protobuf::Nats::NatsClient.new
        @nats.connect(::Protobuf::Nats.config.connection_options)

        @thread_pool = ::Protobuf::Nats::ThreadPool.new(@options[:threads], :max_queue => max_queue_size)

        @subscriptions = []
      end

      def max_queue_size
        if ::ENV.key?("PB_NATS_SERVER_MAX_QUEUE_SIZE")
          ::ENV["PB_NATS_SERVER_MAX_QUEUE_SIZE"].to_i
        else
          @options[:threads]
        end
      end

      def service_klasses
        ::Protobuf::Rpc::Service.implemented_services.map(&:safe_constantize)
      end

      def enqueue_request(request_data, reply_id)
        was_enqueued = thread_pool.push do
          begin
            # Process request.
            response_data = handle_request(request_data)
            # Publish response.
            nats.publish(reply_id, response_data)
          rescue => error
            ::Protobuf::Nats.log_error(error)
          end
        end

        # Publish an ACK to signal the server has picked up the work.
        nats.publish(reply_id, ::Protobuf::Nats::Messages::ACK) if was_enqueued

        was_enqueued
      end

      def subscribe_to_services
        logger.info "Creating subscriptions:"

        service_klasses.each do |service_klass|
          service_klass.rpcs.each do |service_method, _|
            # Skip services that are not implemented.
            next unless service_klass.method_defined? service_method

            subscription_key_and_queue = ::Protobuf::Nats.subscription_key(service_klass, service_method)
            logger.info "  - #{subscription_key_and_queue}"

            subscriptions << nats.subscribe(subscription_key_and_queue, :queue => subscription_key_and_queue) do |request_data, reply_id, _subject|
              unless enqueue_request(request_data, reply_id)
                logger.error { "Thread pool is full! Dropping message for: #{subscription_key_and_queue}" }
              end
            end
          end
        end
      end

      def run
        nats.on_reconnect do
          logger.warn "Reconnected to NATS server!"
        end

        nats.on_disconnect do
          logger.warn "Disconnected from NATS server!"
        end

        nats.on_error do |error|
          ::Protobuf::Nats.log_error(error)
        end

        nats.on_close do
          logger.warn "NATS connection was closed!"
        end

        subscribe_to_services

        yield if block_given?

        loop do
          break unless @running
          sleep 1
        end

        logger.info "Unsubscribing from rpc routes..."
        subscriptions.each do |subscription_id|
          nats.unsubscribe(subscription_id)
        end

        logger.info "Waiting up to 60 seconds for the thread pool to finish shutting down..."
        thread_pool.shutdown
        thread_pool.wait_for_termination(60)
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
