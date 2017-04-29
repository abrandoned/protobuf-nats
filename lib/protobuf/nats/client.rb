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

      # This is an alternative to depending on #failure for invoking the error callback.
      # We never want to silence an error in the event that no callback is provided.
      def failure_handler
        yield
      rescue => error
        logger.debug { sign_message("Server failed request (invoking on_failure): #{error.inspect}") }

        begin
          if @failure_cb
            @failure_cb.call(error)
          else
            raise
          end
        ensure
          # Complete stats and log
          complete
        end
      end

      def ack_timeout
        @ack_timeout ||= if ::ENV.key?("PB_NATS_CLIENT_ACK_TIMEOUT")
          ::ENV["PB_NATS_CLIENT_ACK_TIMEOUT"].to_i
        else
          5
        end
      end

      def reconnect_delay
        @reconnect_delay ||= if ::ENV.key?("PB_NATS_CLIENT_RECONNECT_DELAY")
          ::ENV["PB_NATS_CLIENT_RECONNECT_DELAY"].to_i
        else
          ack_timeout
        end
      end

      def response_timeout
        @response_timeout ||= if ::ENV.key?("PB_NATS_CLIENT_RESPONSE_TIMEOUT")
          ::ENV["PB_NATS_CLIENT_RESPONSE_TIMEOUT"].to_i
        else
          60
        end
      end

      def send_request
        failure_handler do
          begin
            retries ||= 3

            setup_connection
            request_options = {:timeout => response_timeout, :ack_timeout => ack_timeout}
            @response_data = nats_request_with_two_responses(cached_subscription_key, @request_data, request_options)
            parse_response
          rescue ::Protobuf::Nats::Errors::IOException => error
            ::Protobuf::Nats.log_error(error)

            delay = reconnect_delay
            logger.warn "An IOException was raised. We are going to sleep for #{delay} seconds."
            sleep delay

            retry if (retries -= 1) > 0
            raise
          rescue ::NATS::IO::Timeout
            # Nats response timeout.
            retry if (retries -= 1) > 0
            raise
          end
        end
      end

      def cached_subscription_key
        klass = @options[:service]
        method_name = @options[:method]

        method_name_cache = self.class.subscription_key_cache[klass] ||= {}
        method_name_cache[method_name] ||= begin
          ::Protobuf::Nats.subscription_key(klass, method_name)
        end
      end

      # The Java nats client offers better message queueing so we're going to use
      # that over locking ourselves. This split in code isn't great, but we can
      # refactor this later.
      if defined? JRUBY_VERSION

        # This is a request that expects two responses.
        # 1. An ACK from the server. We use a shorter timeout.
        # 2. A PB message from the server. We use a longer timoeut.
        def nats_request_with_two_responses(subject, data, opts)
          # Wait for the ACK from the server
          ack_timeout = opts[:ack_timeout] || 5
          # Wait for the protobuf response
          timeout = opts[:timeout] || 60

          nats = ::Protobuf::Nats.client_nats_connection
          inbox = nats.new_inbox

          # Publish to server
          sub = nats.subscribe(inbox, :max => 2)
          nats.publish(subject, data, inbox)

          # Wait for reply
          first_message = nats.next_message(sub, ack_timeout)
          fail ::NATS::IO::Timeout if first_message.nil?
          second_message = nats.next_message(sub, timeout)
          fail ::NATS::IO::Timeout if second_message.nil?

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

          success = has_ack && response
          fail(::NATS::IO::Timeout, subject) unless success

          response
        ensure
          # Ensure we don't leave a subscriptiosn sitting around.
          nats.unsubscribe(sub)
        end

      else

        def nats_request_with_two_responses(subject, data, opts)
          nats = Protobuf::Nats.client_nats_connection
          inbox = nats.new_inbox
          lock = ::Monitor.new
          ack_condition = lock.new_cond
          pb_response_condition = lock.new_cond
          response = nil
          sid = nats.subscribe(inbox, :max => 2) do |message, _, _|
            lock.synchronize do
              case message
              when ::Protobuf::Nats::Messages::ACK
                ack_condition.signal
                next
              else
                response = message
                pb_response_condition.signal
              end
            end
          end

          lock.synchronize do
            # Publish to server
            nats.publish(subject, data, inbox)

            # Wait for the ACK from the server
            ack_timeout = opts[:ack_timeout] || 5
            with_timeout(ack_timeout) { ack_condition.wait(ack_timeout) }

            # Wait for the protobuf response
            timeout = opts[:timeout] || 60
            with_timeout(timeout) { pb_response_condition.wait(timeout) } unless response
          end

          response
        ensure
          # Ensure we don't leave a subscription sitting around.
          nats.unsubscribe(sid) if response.nil?
        end

        # This is a copy of #with_nats_timeout
        def with_timeout(timeout)
          start_time = ::NATS::MonotonicTime.now
          yield
          end_time = ::NATS::MonotonicTime.now
          duration = end_time - start_time
          raise ::NATS::IO::Timeout.new("nats: timeout") if duration > timeout
        end

      end

    end
  end
end
