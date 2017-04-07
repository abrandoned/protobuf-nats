ext_base = ::File.join(::File.dirname(__FILE__), '..', '..', '..', 'ext')

require ::File.join(ext_base, "jars/slf4j-api-1.7.25.jar")
require ::File.join(ext_base, "jars/slf4j-simple-1.7.25.jar")
require ::File.join(ext_base, "jars/gson-2.6.2.jar")
require ::File.join(ext_base, "jars/jnats-1.1-SNAPSHOT.jar")

module Protobuf
  module Nats
    class JNats
      attr_reader :connection

      def initialize
      end

      def connect(*)
        @connection ||= ::Java::IoNatsClient::Nats.connect
      end

      def close
        @connection.close
      end

      def flush(timeout_sec = 0.5)
        @connection.flush(timeout_sec * 1000)
      end

      def next_message(sub, timeout_sec)
        sub.nextMsg(timeout_sec * 1000)
      end

      def publish(subject, data, mailbox = nil)
        # The "true" here is to force flush. May not need this.
        @connection.publish(subject, mailbox, data.to_java_bytes, true)
      end

      def subscribe(subject, options = {}, &block)
        queue = options[:queue]
        max = options[:max]
        sub = if block
                @connection.subscribeAsync(subject, queue) do |message|
                  begin
                    block.call(message.getData.to_s, message.getReplyTo, message.getSubject)
                  rescue => error
                    puts error
                    puts error.backtrace.join("\n")
                  end
                end
              else
                @connection.subscribeSync(subject, queue)
              end

        if max
          sub.autoUnsubscribe(max)
        end

        sub
      end

      def unsubscribe(sub)
        return if sub.nil?
        # The "true" here is to ignore and invalid conn.
        sub.unsubscribe(true)
      end

      def new_inbox
        "_INBOX.#{::SecureRandom.hex(13)}"
      end
    end
  end
end

