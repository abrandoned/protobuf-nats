require "protobuf/nats"
require "protobuf/rpc/server"
require "concurrent"

module Protobuf
  module Nats
    class FfiServer
      include ::Protobuf::Rpc::Server
      include ::Protobuf::Logging

      attr_reader :nats_connection, :thread_pool, :subscriptions

      def self.service_klasses
        ::ObjectSpace.each_object(::Class).select { |klass| klass < ::Protobuf::Rpc::Service }
      end

      def initialize(options)
        @options = options
        @running = true
        @stopped = false

        @nats_connection = create_nats_connection
        @thread_pool = ::Concurrent::FixedThreadPool.new(options[:threads])

        @subscriptions = []
      end

      def create_nats_connection
        opts_pointer = FFI::MemoryPointer.new :pointer
        conn_t = FFI::MemoryPointer.new :pointer
        FFI::Nats::Core.natsOptions_Create(opts_pointer)
        opts_pointer = opts_pointer.get_pointer(0)
        FFI::Nats::Core.natsOptions_SetURL(opts_pointer, "nats://localhost:4222")
        FFI::Nats::Core.natsOptions_UseGlobalMessageDelivery(opts_pointer, true)
        FFI::Nats::Core.natsConnection_Connect(conn_t, opts_pointer)
        conn_t.get_pointer(0)
      end

      def service_klasses
        self.class.service_klasses
      end

      def execute_request_promise(conn_ptr, _subscription, nats_message_ptr, _closure)
        ::Concurrent::Promise.new(:executor => thread_pool).then do
          data_length, _ = FFI::Nats::Core.natsMsg_GetDataLength(nats_message_ptr)
          _, ptr = FFI::Nats::Core.natsMsg_GetData(nats_message_ptr)
          request_data = ptr.read_string(data_length)

          response_data = handle_request(request_data)
          response_data_ptr = FFI::MemoryPointer.new(1, response_data.size)
          response_data_ptr.put_bytes(0, response_data)

          reply_id, _ = FFI::Nats::Core.natsMsg_GetReply(nats_message_ptr)
          FFI::Nats::Core.natsConnection_Publish(conn_ptr, reply_id, response_data_ptr, response_data_ptr.size)
          FFI::Nats::Core.natsConnection_Flush(conn_ptr)

          response_data_ptr.free
          FFI::Nats::Core.natsMsg_Destroy(nats_message_ptr)
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

          subscriber_cb = FFI::Function.new(:void, [:pointer, :pointer, :pointer, :pointer], :blocking => true) do |conn, sub, nats_message_ptr, closure|
            execute_request_promise(conn, sub, nats_message_ptr, closure)
          end

          subscription_ptr = FFI::MemoryPointer.new(:pointer)

          FFI::Nats::Core.natsConnection_QueueSubscribe(subscription_ptr,
                                                        nats_connection,
                                                        subscription_key_and_queue,
                                                        subscription_key_and_queue,
                                                        subscriber_cb, nil)
          subscriptions << subscription_ptr
          FFI::Nats::Core.natsConnection_Flush(nats_connection)
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

        logger.info "Unsubscribing..."
        subscriptions.each do |subscription_ptr|
          FFI::Nats::Core.natsSubscription_Unsubscribe(subscription_ptr.get_pointer(0))
        end

        logger.info "Waiting for requests to complete..."
        thread_pool.shutdown
        thread_pool.wait_for_termination

        logger.info "Cleaning up subscriptions..."
        subscriptions.each do |subscription_ptr|
          FFI::Nats::Core.natsSubscription_Destroy(subscription_ptr.get_pointer(0))
        end

        logger.info "Cleaning up NATS connection..."
        FFI::Nats::Core.natsConnection_Close(nats_connection)
        FFI::Nats::Core.natsConnection_Destroy(nats_connection)
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
