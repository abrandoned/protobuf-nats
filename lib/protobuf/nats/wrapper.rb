require "ffi/nats/core"

module Protobuf
  module Nats
    class Wrapper
      DEFAULT_SERVERS = ["nats://localhost:4222"]
      NATS_OK = ::FFI::Nats::Core::NATS_STATUS[:NATS_OK]
      NATS_TIMEOUT = ::FFI::Nats::Core::NATS_STATUS[:NATS_TIMEOUT]

      class Message
        attr_reader :subject, :reply, :data
        def initialize(message_ptr)
          @subject, _ = ::FFI::Nats::Core.natsMsg_GetSubject(message_ptr)
          @reply, _ = ::FFI::Nats::Core.natsMsg_GetReply(message_ptr)
          data_length = ::FFI::Nats::Core.natsMsg_GetDataLength(message_ptr)
          _, data_ptr = ::FFI::Nats::Core.natsMsg_GetData(message_ptr)
          @data = data_ptr.read_bytes(data_length)
        end
      end

      def connect(opts = {})
        servers = opts.fetch(:servers, DEFAULT_SERVERS)

        options_ptr = ::FFI::MemoryPointer.new(:pointer)
        check ::FFI::Nats::Core.natsOptions_Create(options_ptr)
        @options_ptr = options_ptr.read_pointer
        create_servers_ptr(servers) do |ptr|
          check ::FFI::Nats::Core.natsOptions_SetServers(@options_ptr, ptr, servers.size)
        end
        check ::FFI::Nats::Core.natsOptions_UseGlobalMessageDelivery(@options_ptr, true)

        connection_ptr = ::FFI::MemoryPointer.new(:pointer)
        check ::FFI::Nats::Core.natsConnection_Connect(connection_ptr, @options_ptr)
        @connection_ptr = connection_ptr.read_pointer

        flush

        true
      end

      def close
        ::FFI::Nats::Core.natsConnection_Destroy(@connection_ptr) if @connection_ptr
        ::FFI::Nats::Core.natsOptions_Destroy(@options_ptr) if @options_ptr
      end

      def flush
        check ::FFI::Nats::Core.natsConnection_FlushTimeout(@connection_ptr, 5_000)
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
        subscription_ptr = ::FFI::MemoryPointer.new(:pointer)
        if block
          callback = create_callback do |message_ptr|
            message = Message.new(message_ptr)
            block.call(message.data, message.reply, message.subject)
          end
          check ::FFI::Nats::Core.natsConnection_Subscribe(subscription_ptr, @connection_ptr, subject, callback, nil)
        else
          check ::FFI::Nats::Core.natsConnection_SubscribeSync(subscription_ptr, @connection_ptr, subject)
        end
        subscription_ptr.read_pointer
      end

      def next_message(subscription_ptr, timeout = 500)
        return nil if nullptr?(subscription_ptr)

        message = nil
        ::FFI::MemoryPointer.new(:pointer) do |message_ptr|
          code = ::FFI::Nats::Core.natsSubscription_NextMsg(message_ptr, subscription_ptr, timeout)
          return nil if code == NATS_TIMEOUT
          check(code)
          message = Message.new(message_ptr.read_pointer)
        end
        message
      end

      def new_inbox
        "_INBOX.#{::SecureRandom.hex(13)}"
      end

      def unsubscribe(subscription_ptr, options = {})
        return nil if nullptr?(subscription_ptr)
        ::FFI::Nats::Core.natsSubscription_Unsubscribe(subscription_ptr)
        ::FFI::Nats::Core.natsSubscription_Destroy(subscription_ptr)
      end

    private

      def nullptr?(ptr)
        return false if ptr.nil?
        return false unless ptr.is_a?(::FFI::Pointer)
        ptr.null?
      end

      def create_callback(block)
        ::FFI::Function.new(:void, [:pointer, :pointer, :pointer, :pointer], :blocking => true) do |_, _, message_ptr, _|
          yield(msg)
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
    end
  end
end
