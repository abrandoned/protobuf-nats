require "protobuf/nats"
require "protobuf/rpc/connectors/base"
require "monitor"

module Protobuf
  module Nats
    class Client < ::Protobuf::Rpc::Connectors::Base
      def initialize(options)
        # may need to override to setup connection at this stage ... may also do on load of class
        super

        # This will ensure the client is started.
        ::Protobuf::Nats.start_client_nats_connection
      end

      def close_connection
        # no-op (I think for now), the connection to server is persistent
      end

      def self.subscription_key_cache
        @subscription_key_cache ||= {}
      end

      def send_request
        retries ||= 3

        setup_connection
        @response_data = nats_request_with_two_responses(cached_subscription_key, @request_data, {})
        parse_response
      rescue ::NATS::IO::Timeout
        # Nats response timeout.
        retry if (retries -= 1) > 0
        raise
      end

      def cached_subscription_key
        klass = @options[:service]
        method_name = @options[:method]

        method_name_cache = self.class.subscription_key_cache[klass] ||= {}
        method_name_cache[method_name] ||= begin
          ::Protobuf::Nats.subscription_key(klass, method_name)
        end
      end

      # # This is a request that expects two responses.
      # # 1. An ACK from the server. We use a shorter timeout.
      # # 2. A PB message from the server. We use a longer timoeut.
      # def nats_request_with_two_responses(subject, data, opts)
      #   nats = Protobuf::Nats.client_nats_connection
      #   inbox = nats.new_inbox
      #   lock = ::Monitor.new
      #   ack_condition = lock.new_cond
      #   pb_response_condition = lock.new_cond
      #   response = nil
      #   sub_ptr = nats.subscribe(inbox, :no_delay => true, :max => 2) do |message, _, _|
      #     lock.synchronize do
      #       case message
      #       when ::Protobuf::Nats::Messages::ACK
      #         ack_condition.signal
      #         next
      #       else
      #         response = message
      #         pb_response_condition.signal
      #       end
      #     end
      #   end

      #   # Publish to server
      #   nats.publish(subject, data, inbox)

      #   lock.synchronize do
      #     # Wait for the ACK from the server
      #     ack_timeout = opts[:ack_timeout] || 5_000
      #     with_timeout(ack_timeout) { ack_condition.wait(ack_timeout) }

      #     # Wait for the protobuf response
      #     timeout = opts[:timeout] || 60_000
      #     with_timeout(timeout) { pb_response_condition.wait(timeout) } unless response
      #   end

      #   completed_both = true

      #   response
      # ensure
      #   # Ensure we don't leave a subscription sitting around.
      #   nats.unsubscribe(sub_ptr) unless completed_both
      # end

      # # This is a copy of #with_nats_timeout
      # def with_timeout(timeout)
      #   start_time = ::NATS::MonotonicTime.now
      #   yield
      #   end_time = ::NATS::MonotonicTime.now
      #   duration = end_time - start_time
      #   raise ::NATS::IO::Timeout.new("nats: timeout") if duration > timeout
      # end

      # This is a request that expects two responses.
      # 1. An ACK from the server. We use a shorter timeout.
      # 2. A PB message from the server. We use a longer timoeut.
      def nats_request_with_two_responses(subject, data, opts)
        # Wait for the ACK from the server
        ack_timeout = opts[:ack_timeout] || 5_000
        # Wait for the protobuf response
        timeout = opts[:timeout] || 60_000

        nats = Protobuf::Nats.client_nats_connection
        inbox = nats.new_inbox

        # ::Protobuf::Logging.logger.info "START FOR #{inbox}"
        # Publish to server
        nats.publish(subject, data, inbox)
        # ::Protobuf::Logging.logger.info "PUB FOR #{inbox}"

        # Wait for reply
        sub_ptr = nats.subscribe(inbox, :max => 2, :no_delay => true)
        # ::Protobuf::Logging.logger.info "SUB FOR #{inbox}"
        first_message = nats.next_message(sub_ptr, ack_timeout)
        fail ::NATS::IO::Timeout if first_message.nil?
        # ::Protobuf::Logging.logger.info "RECEIVED MESSAGE ONE FOR #{inbox} - #{first_message.data.size}"
        second_message = nats.next_message(sub_ptr, timeout)
        fail ::NATS::IO::Timeout if second_message.nil?
        # ::Protobuf::Logging.logger.info "RECEIVED MESSAGE TWO FOR #{inbox} - #{second_message.data.size}"

        # Check messages
        response = nil
        has_ack = false
        case first_message.data
        when ::Protobuf::Nats::Messages::ACK then has_ack = true
        else response = first_message.data
        end
        case second_message.data
        when ::Protobuf::Nats::Messages::ACK then has_ack = true
        else response = second_message.data
        end

        two_messages = true
        success = has_ack && response
        fail ::NATS::IO::Timeout, "YOU NEED MORE DATA @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@" unless success

        # ::Protobuf::Logging.logger.info "DONE FOR #{inbox}"
        response
      ensure
        # Ensure we don't leave a subscriptiosn sitting around.
        # This also cleans up memory.
        nats.unsubscribe(sub_ptr) unless two_messages
        # ::Protobuf::Logging.logger.info "UNSUB FOR #{inbox}"
      end

    end
  end
end
