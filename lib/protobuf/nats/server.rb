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
        @processing_requests = true
        @running = true
        @stopped = false

        @nats = @options[:client] || ::Protobuf::Nats::NatsClient.new
        @nats.connect(::Protobuf::Nats.config.connection_options)

        @thread_pool = ::Protobuf::Nats::ThreadPool.new(@options[:threads], :max_queue => max_queue_size)

        @subscriptions = []
        @server = options.fetch(:server, ::Socket.gethostname)
      end

      def max_queue_size
        ::ENV.fetch("PB_NATS_SERVER_MAX_QUEUE_SIZE", @options[:threads]).to_i
      end

      def slow_start_delay
        @slow_start_delay ||= ::ENV.fetch("PB_NATS_SERVER_SLOW_START_DELAY", 10).to_i
      end

      def subscriptions_per_rpc_endpoint
        @subscriptions_per_rpc_endpoint ||= ::ENV.fetch("PB_NATS_SERVER_SUBSCRIPTIONS_PER_RPC_ENDPOINT", 10).to_i
      end

      def service_klasses
        ::Protobuf::Rpc::Service.implemented_services.map(&:safe_constantize)
      end

      def enqueue_request(request_data, reply_id)
        was_enqueued = thread_pool.push do
          begin
            # Process request.
            response_data = handle_request(request_data, 'server' => @server)
            # Publish response.
            nats.publish(reply_id, response_data)
          rescue => error
            ::Protobuf::Nats.notify_error_callbacks(error)
          end
        end

        # Publish an ACK to signal the server has picked up the work.
        nats.publish(reply_id, ::Protobuf::Nats::Messages::ACK) if was_enqueued

        was_enqueued
      end

      def pause_file_path
        ::ENV.fetch("PB_NATS_SERVER_PAUSE_FILE_PATH", nil)
      end

      def print_subscription_keys
        logger.info "Creating subscriptions:"

        with_each_subscription_key do |subscription_key|
          logger.info "  - #{subscription_key}"
        end
      end

      def subscribe_to_services_once
        with_each_subscription_key do |subscription_key_and_queue|
          subscriptions << nats.subscribe(subscription_key_and_queue, :queue => subscription_key_and_queue) do |request_data, reply_id, _subject|
            unless enqueue_request(request_data, reply_id)
              logger.error { "Thread pool is full! Dropping message for: #{subscription_key_and_queue}" }
            end
          end
        end
      end

      def with_each_subscription_key
        fail ::ArgumentError unless block_given?

        service_klasses.each do |service_klass|
          service_klass.rpcs.each do |service_method, _|
            # Skip services that are not implemented.
            next unless service_klass.method_defined? service_method

            yield ::Protobuf::Nats.subscription_key(service_klass, service_method)
          end
        end
      end

      # Slow start subscriptions by adding X rounds of subz every
      # Y seconds, where X is subscriptions_per_rpc_endpoint and Y is
      # slow_start_delay.
      def finish_slow_start
        logger.info "Slow start has started..."
        completed = 1

        # We have (X - 1) here because we always subscribe at least once.
        (subscriptions_per_rpc_endpoint - 1).times do
          next unless @running
          next if paused?
          completed += 1
          sleep slow_start_delay
          subscribe_to_services_once
          logger.info "Slow start adding another round of subscriptions (#{completed}/#{subscriptions_per_rpc_endpoint})..."
        end

        logger.info "Slow start finished."
      end

      def detect_and_handle_a_pause
        case
        # If we are taking requests and detect a pause file, then unsubscribe.
        when @processing_requests && paused?
          @processing_requests = false
          logger.warn("Pausing server!")
          unsubscribe

        # If we were paused and the pause file is no longer present, then subscribe again.
        when !@processing_requests && !paused?
          logger.warn("Resuming server: resubscribing to all services and restarting slow start!")
          @processing_requests = true
          subscribe
        end
      end

      def paused?
        !pause_file_path.nil? && ::File.exist?(pause_file_path)
      end

      def run
        nats.on_reconnect do
          logger.warn "Server NATS connection was reconnected"
        end

        nats.on_disconnect do
          logger.warn "Server NATS connection was disconnected"
        end

        nats.on_error do |error|
          ::Protobuf::Nats.notify_error_callbacks(error)
        end

        nats.on_close do
          logger.warn "Server NATS connection was closed"
        end

        print_subscription_keys
        subscribe { yield if block_given? }

        loop do
          break unless @running
          detect_and_handle_a_pause
          sleep 1
        end

        unsubscribe

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

      def subscribe
        subscribe_to_services_once
        yield if block_given?
        finish_slow_start
      end

      def unsubscribe
        logger.info "Unsubscribing from rpc routes..."
        subscriptions.each do |subscription_id|
          nats.unsubscribe(subscription_id)
        end
      end
    end
  end
end
