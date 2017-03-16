require "protobuf/rpc/connectors/base"

module Protobuf
  module Nats
    class Client < ::Protobuf::Rpc::Connectors::Base

      def initialize(options)
        # may need to override to setup connection at this stage ... may also do on load of class
        super
      end

      def close_connection
        # no-op (I think for now), the connection to server is persistent
      end

      def send_request
        # after method execution the "response" should be in @response_data and should be a string of bytes
      end

    end
  end
end
