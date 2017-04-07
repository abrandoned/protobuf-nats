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

        @nats = options[:client] || ::NATS::IO::Client.new
        @nats.connect(::Protobuf::Nats.config.connection_options)

        # @thread_pool = ::Concurrent::FixedThreadPool.new(options[:threads], :max_queue => options[:threads])
        @thread_pool = ::Protobuf::Nats::ThreadPool.new(options[:threads], :max_queue => options[:threads])

        @subscriptions = []
      end

      def service_klasses
        ::Protobuf::Rpc::Service.implemented_services.map(&:safe_constantize)
      end

     #  def execute_request_promise(request_data, reply_id)
     #    promise = ::Concurrent::Promise.new(:executor => thread_pool).then do
     #      # Process request.
     #      response_data = handle_request(request_data)
     #      # Publish response.
     #      nats.publish(reply_id, response_data)
     #    end.on_error do |error|
     #      log_error(error)
     #    end.execute

     #    # Publish an ACK to signal the server has picked up the work.
     #    nats.publish(reply_id, ::Protobuf::Nats::Messages::ACK)

     #    promise
     #  rescue ::Concurrent::RejectedExecutionError
     #    nil
     #  end

      def execute_request_promise(request_data, reply_id)
        thread_pool.push do
          # Publish an ACK to signal the server has picked up the work.
          nats.publish(reply_id, ::Protobuf::Nats::Messages::ACK)
          # Process request.
          response_data = handle_request(request_data)
          # Publish response.
          nats.publish(reply_id, response_data)
        end
      end

      def log_error(error)
        logger.error error.to_s
        if error.respond_to?(:backtrace) && error.backtrace.is_a?(::Array)
          logger.error error.backtrace.join("\n")
        end
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
              unless execute_request_promise(request_data, reply_id)
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
          log_error(error)
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
