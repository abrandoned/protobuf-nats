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

      def ack_timeout
        @ack_timeout ||= if ::ENV.key?("PB_NATS_CLIENT_ACK_TIMEOUT")
          ::ENV["PB_NATS_CLIENT_ACK_TIMEOUT"].to_i
        else
          5
        end
      end

      def nack_backoff_intervals
        @nack_backoff_intervals ||= if ::ENV.key?("PB_NATS_CLIENT_NACK_BACKOFF_INTERVALS")
          ::ENV["PB_NATS_CLIENT_NACK_BACKOFF_INTERVALS"].split(",").map(&:to_i)
        else
          [0, 1, 3, 5, 10]
        end
      end

      def nack_backoff_splay
        @nack_backoff_splay ||= if nack_backoff_splay_limit > 0
          rand(nack_backoff_splay_limit)
        else
          0
        end
      end

      def nack_backoff_splay_limit
        @nack_backoff_splay_limit ||= if ::ENV.key?("PB_NATS_CLIENT_NACK_BACKOFF_SPLAY_LIMIT")
          ::ENV["PB_NATS_CLIENT_NACK_BACKOFF_SPLAY_LIMIT"].to_i
        else
          10
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
        retries ||= 3
        nack_retry ||= 0

        loop do
          setup_connection
          request_options = {:timeout => response_timeout, :ack_timeout => ack_timeout}
          @response_data = nats_request_with_two_responses(cached_subscription_key, @request_data, request_options)
          case @response_data
          when :ack_timeout
            next if (retries -= 1) > 0
            raise ::NATS::IO::Timeout
          when :nack
            interval = nack_backoff_intervals[nack_retry]
            nack_retry += 1
            raise ::NATS::IO::Timeout if interval.nil?
            sleep((interval + nack_backoff_splay)/1000.0)
            next
          end
          break
        end

        parse_response

      rescue ::Protobuf::Nats::Errors::IOException => error
        ::Protobuf::Nats.log_error(error)

        delay = reconnect_delay
        logger.warn "An IOException was raised. We are going to sleep for #{delay} seconds."
        sleep delay

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
          return :ack_timeout if first_message.nil?
          return :nack if first_message.data == ::Protobuf::Nats::Messages::NACK

          second_message = nats.next_message(sub, timeout)
          fail(::NATS::IO::Timeout, subject) if second_message.nil?

          # Check messages
          response = case ::Protobuf::Nats::Messages::ACK
                     when first_message.data then second_message.data
                     when second_message.data then first_message.data
                     end

          fail(::NATS::IO::Timeout, subject) unless response

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
          received = lock.new_cond
          messages = []
          first_message = nil
          second_message = nil
          response = nil
          has_ack = false

          sid = nats.subscribe(inbox, :max => 2) do |message, _, _|
            lock.synchronize do
              messages << message
              received.signal
            end
          end

          lock.synchronize do
            # Publish to server
            nats.publish(subject, data, inbox)

            # Wait for the ACK from the server
            ack_timeout = opts[:ack_timeout] || 5
            received.wait(ack_timeout) if messages.empty?
            first_message = messages.shift

            return :ack_timeout if first_message.nil?
            return :nack if first_message == ::Protobuf::Nats::Messages::NACK

            # Wait for the protobuf response
            timeout = opts[:timeout] || 60
            received.wait(timeout) if messages.empty?
            second_message = messages.shift
          end

          response = case ::Protobuf::Nats::Messages::ACK
                     when first_message then second_message
                     when second_message then first_message
                     end

          fail(::NATS::IO::Timeout, subject) unless response

          response
        ensure
          # Ensure we don't leave a subscription sitting around.
          nats.unsubscribe(sid) if response.nil?
        end

      end

    end
  end
end
