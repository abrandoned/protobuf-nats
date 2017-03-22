require "protobuf/nats"
require "protobuf/rpc/connectors/base"

module Protobuf
  module Nats
    class FfiClient < ::Protobuf::Rpc::Connectors::Base
      def initialize(options)
        # may need to override to setup connection at this stage ... may also do on load of class
        super

        Protobuf::Nats.ensure_client_nats_connection_started
      end

      def close_connection
        # no-op (I think for now), the connection to server is persistent
      end

      def subscription_key
        "#{@options[:service]}::#{@options[:method]}"
      end

      def send_request
        retries ||= 3
        setup_connection

        nats_message_ptr = FFI::MemoryPointer.new(:pointer)
        request_data_ptr = FFI::MemoryPointer.new(1, @request_data.size)
        request_data_ptr.put_bytes(0, @request_data)

        FFI::Nats::Core.natsConnection_Request(nats_message_ptr,
                                               ::Protobuf::Nats::Config.client_nats_connection,
                                               subscription_key,
                                               request_data_ptr,
                                               request_data_ptr.size,
                                               60_000)

        data_length, _ = FFI::Nats::Core.natsMsg_GetDataLength(nats_message_ptr.get_pointer(0))
        _, ptr = FFI::Nats::Core.natsMsg_GetData(nats_message_ptr.get_pointer(0))
        @response_data = ptr.read_string(data_length)
        response = parse_response

        FFI::Nats::Core.natsMsg_Destroy(nats_message_ptr.get_pointer(0))
        request_data_ptr.free
        # TODO: Shouldn't the destroy take care of this?
        nats_message_ptr.free

        response
      end

    end
  end
end
