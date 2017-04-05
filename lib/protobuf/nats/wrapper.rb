require "ffi/nats/core"

module Protobuf
  module Nats
    class Wrapper
      NATS_OK = ::FFI::Nats::Core::NATS_STATUS[:NATS_OK]
      NATS_TIMEOUT = ::FFI::Nats::Core::NATS_STATUS[:NATS_TIMEOUT]

      class Message
        attr_reader :subject, :reply, :data
        def initialize(message_ptr)
          @subject, _ = ::FFI::Nats::Core.natsMsg_GetSubject(message_ptr)
          @subject = @subject.dup

          @reply, _ = ::FFI::Nats::Core.natsMsg_GetReply(message_ptr)
          @reply = @reply.dup

          data_length = ::FFI::Nats::Core.natsMsg_GetDataLength(message_ptr)
          _, data_ptr = ::FFI::Nats::Core.natsMsg_GetData(message_ptr)
          @data = data_ptr.read_bytes(data_length).dup
        end
      end

      def initialize
        @error_cb = lambda {|_error_code|}
        @reconnect_cb = lambda {|_error_code|}
        @disconnect_cb = lambda {|_error_code|}

        # We need to store refs so these function pointers are not GCd.
        @sub_cb = {}
        @options_cb = {}
      end

      def connect(config = nil)
        config ||= ::Protobuf::Nats.config
        servers = config.servers

        options_ptr = ::FFI::MemoryPointer.new(:pointer)
        check ::FFI::Nats::Core.natsOptions_Create(options_ptr)
        options_ptr = options_ptr.read_pointer
        create_servers_ptr(servers) do |ptr|
          check ::FFI::Nats::Core.natsOptions_SetServers(options_ptr, ptr, servers.size)
        end
        check ::FFI::Nats::Core.natsOptions_UseGlobalMessageDelivery(options_ptr, true)

        error_cb = @options_cb["error"] = create_error_callback do |error_code|
          @error_cb.call(error_code)
        end
        check ::FFI::Nats::Core.natsOptions_SetErrorHandler(options_ptr, error_cb, nil)
        disconnect_cb = @options_cb["disconnect"] = create_connect_callback do
          @disconnect_cb.call
        end
        check ::FFI::Nats::Core.natsOptions_SetDisconnectedCB(options_ptr, disconnect_cb, nil)
        reconnect_cb = @options_cb["reconnect"] = create_connect_callback do
          @reconnect_cb.call
        end
        check ::FFI::Nats::Core.natsOptions_SetReconnectedCB(options_ptr, reconnect_cb, nil)

        apply_tls_options(options_ptr, config) if config.uses_tls

        connection_ptr = ::FFI::MemoryPointer.new(:pointer)
        check ::FFI::Nats::Core.natsConnection_Connect(connection_ptr, options_ptr)
        @connection_ptr = connection_ptr.read_pointer

        ::FFI::Nats::Core.natsOptions_Destroy(options_ptr)

        flush

        true
      end

      def close
        ::FFI::Nats::Core.natsConnection_Destroy(@connection_ptr) if @connection_ptr
      end

      def flush(timeout = 500)
        check ::FFI::Nats::Core.natsConnection_FlushTimeout(@connection_ptr, timeout)
        true
      end

      def publish(subject, data, reply = nil)
        ::FFI::MemoryPointer.new(1, data.size) do |data_ptr|
          data_ptr.write_bytes(data)
          if reply
            check ::FFI::Nats::Core.natsConnection_PublishRequest(@connection_ptr, subject, reply, data_ptr, data_ptr.size)
          else
            check ::FFI::Nats::Core.natsConnection_Publish(@connection_ptr, subject, data_ptr, data_ptr.size)
          end
        end

        true
      end

      def subscribe(subject, options = {}, &block)
        max = options[:max]
        queue = options[:queue]
        no_delay = options.fetch(:no_delay, false)
        subscription_ptr = ::FFI::MemoryPointer.new(:pointer)

        callback = nil
        if block
          callback = @sub_cb[subject] = create_callback do |message_ptr|
            message = Message.new(message_ptr)
            block.call(message.data, message.reply, message.subject)
            ::FFI::Nats::Core.natsMsg_Destroy(message_ptr)
          end
        end

        if queue
          if block
            check ::FFI::Nats::Core.natsConnection_QueueSubscribe(subscription_ptr, @connection_ptr, subject, queue, callback, nil)
          else
            check ::FFI::Nats::Core.natsConnection_QueueSubscribeSync(subscription_ptr, @connection_ptr, subject, queue)
          end
        else
          if block
            check ::FFI::Nats::Core.natsConnection_Subscribe(subscription_ptr, @connection_ptr, subject, callback, nil)
          else
            check ::FFI::Nats::Core.natsConnection_SubscribeSync(subscription_ptr, @connection_ptr, subject)
          end
        end

        subscription_ptr = subscription_ptr.read_pointer
        if max
          check ::FFI::Nats::Core.natsSubscription_AutoUnsubscribe(subscription_ptr, max)
        end

        if no_delay
          check ::FFI::Nats::Core.natsSubscription_NoDeliveryDelay(subscription_ptr)
        end
        subscription_ptr
      end

      def next_message(subscription_ptr, timeout = 500)
        return nil if nullptr?(subscription_ptr)

        message = nil
        ::FFI::MemoryPointer.new(:pointer) do |message_ptr|
          code = ::FFI::Nats::Core.natsSubscription_NextMsg(message_ptr, subscription_ptr, timeout)
          if code == NATS_TIMEOUT
            return nil
          end

          check(code)
          message = Message.new(message_ptr.read_pointer)
          ::FFI::Nats::Core.natsMsg_Destroy(message_ptr.read_pointer)
        end
        message
      end

      def on_error(&block)
        @error_cb = block
      end

      def on_disconnect(&block)
        @disconnect_cb = block
      end

      def on_reconnect(&block)
        @reconnect_cb = block
      end

      def new_inbox
        "_INBOX.#{::SecureRandom.hex(13)}"
      end

      def unsubscribe(subscription_ptr, options = {})
        ::FFI::Nats::Core.natsSubscription_Unsubscribe(subscription_ptr)
        ::FFI::Nats::Core.natsSubscription_Destroy(subscription_ptr)
      end

    private

      def nullptr?(ptr)
        return false if ptr.nil?
        return false unless ptr.is_a?(::FFI::Pointer)
        ptr.null?
      end

      def create_callback
        ::FFI::Function.new(:void, [:pointer, :pointer, :pointer, :pointer], :blocking => true) do |_, _, message_ptr, _|
          yield(message_ptr)
        end
      end

      def create_connect_callback
        ::FFI::Function.new(:void, [:pointer, :pointer], :blocking => true) do |_, _|
          yield
        end
      end

      def create_error_callback
        ::FFI::Function.new(:void, [:pointer, :pointer, :int, :pointer], :blocking => true) do |_, _, error_code, _|
          yield(error_code)
        end
      end

      def create_servers_ptr(servers)
        server_pointers = servers.map { |uri| ::FFI::MemoryPointer.from_string(uri) }
        ptr = ::FFI::MemoryPointer.new(:strptr, servers.size)
        ptr.write_array_of_pointer(server_pointers)
        yield(ptr)
        server_pointers.each { |server_ptr| server_ptr.free }
        ptr.free
        true
      end

      def check(response_code)
        return if response_code == NATS_OK
        enum = ::FFI::Nats::Core::NATS_STATUS.find(response_code)
        fail "Received bad response code from cnats: #{response_code} - #{enum}"
      end

      def apply_tls_options(options_ptr, config)
        cert = config.tls_client_cert
        key = config.tls_client_key
        return unless cert && key
        check ::FFI::Nats::Core.natsOptions_LoadCertificatesChain(options_ptr, config.tls_client_cert, config.tls_client_key)
      end
    end
  end
end
