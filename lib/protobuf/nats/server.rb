require "active_support/core_ext/class/subclasses"
require "protobuf/rpc/server"
require "protobuf/rpc/service"
require "protobuf/nats/tp"

require "ruby-prof"
require 'ruby-prof-flamegraph'

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

        @nats = options[:client] || ::Protobuf::Nats::Wrapper.new
        @nats.connect(::Protobuf::Nats.config.connection_options)

        #@thread_pool = ::Concurrent::FixedThreadPool.new(options[:threads], :max_queue => options[:threads])
        @thread_pool = ::Protobuf::Nats::ThreadPool.new(options[:threads], :max_queue => options[:threads])

        @subscriptions = []
      end

      def service_klasses
        ::Protobuf::Rpc::Service.implemented_services.map(&:safe_constantize)
      end

      def execute_request_promise(request_data, reply_id)
        thread_pool.push do
          begin
            # logger.info "INSIDE POOL FOR #{reply_id}"
            #puts "here: #{reply_id}"
            # Publish an ACK to signal the server has picked up the work.
            # logger.info "PUBLISHED ACK FOR #{reply_id}"
            nats.publish(reply_id, ::Protobuf::Nats::Messages::ACK)

            # Process request.
            response_data = handle_request(request_data)
            # logger.info "PROCESSED REQUEST FOR #{reply_id}"
            # Publish response.
            nats.publish(reply_id, response_data)
            nats.flush
            # puts "DONE PUB: #{reply_id}"
            # logger.info "PUBLISHED RESPONSE FOR #{reply_id}"
          rescue => error
            log_error(error)
          end
        end
      end

      # def execute_request_promise(request_data, reply_id)
      #   # logger.info "ADD TO POOL FOR #{reply_id}"
      #   promise = ::Concurrent::Promise.new(:executor => thread_pool).then do
      #     # logger.info "INSIDE POOL FOR #{reply_id}"

      #     Publish an ACK to signal the server has picked up the work.
      #     logger.info "PUBLISHED ACK FOR #{reply_id}"
      #     nats.publish(reply_id, ::Protobuf::Nats::Messages::ACK)

      #     Process request.
      #     response_data = handle_request(request_data)
      #     logger.info "PROCESSED REQUEST FOR #{reply_id}"
      #     Publish response.
      #     nats.publish(reply_id, response_data)
      #     logger.info "PUBLISHED RESPONSE FOR #{reply_id}"
      #   end.on_error do |error|
      #     log_error(error)
      #     logger.info "ERROR FOR #{reply_id} - #{error}"
      #   end.execute

      #   promise
      # rescue ::Concurrent::RejectedExecutionError
      #   logger.error "POOL IS FULL AT #{reply_id}"
      #   nil
      # end

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

            subscriptions << nats.subscribe(subscription_key_and_queue, :no_delay => true, :queue => subscription_key_and_queue) do |request_data, reply_id, _subject|
              unless execute_request_promise(request_data, reply_id)
                logger.error { "Thread pool is full! Dropping message for: #{subscription_key_and_queue}" }
              end

              @request_count += 1
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

        nats.on_error do |error_code|
          enum = ::FFI::Nats::Core::NATS_STATUS.find(error_code)
          logger.error "Received error from cnats: #{error_code} - #{enum}"
        end

        subscribe_to_services

        yield if block_given?

        #::RubyProf.start

        @request_count = 0
        loop do
          break unless @running
          sleep 1
          # if @request_count > 5_000
          #   STDERR.puts "writing now...!"
          #   result = ::RubyProf.stop
          #   file = File.open("/tmp/server-flame-prof-nats.prof", "w+")
          #   printer = RubyProf::FlameGraphPrinter.new(result)
          #   printer.print(file, {})
          #   file.close
          #   break
          # end
        end

        logger.info "Unsubscribing from rpc routes..."
        subscriptions.each do |subscription_ptr|
          nats.unsubscribe(subscription_ptr)
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
