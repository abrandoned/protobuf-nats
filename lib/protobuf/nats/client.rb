require "protobuf/rpc/connectors/base"

module Protobuf
  module Nats
    class Client < ::Protobuf::Rpc::Connectors::Base
      def initialize(options)
        # may need to override to setup connection at this stage ... may also do on load of class
        super

        Protobuf::Nats.ensure_client_pools_started
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
        Protobuf::Nats::Config.client_nats_pool.with do |nats|
          @response_data = nil
          nats_message = nats.request(subscription_key, @request_data, timeout: 60)
          @response_data = nats_message.data
        end
        parse_response
      rescue ::Timeout::Error
        # Connection pool timeout getting a nats client.
        retry if (retries -= 1) > 0
        raise
      rescue ::NATS::IO::Timeout
        # Nats response timeout.
        retry if (retries -= 1) > 0
        raise
      end

    end
  end
end
