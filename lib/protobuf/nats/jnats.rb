ext_base = ::File.join(::File.dirname(__FILE__), '..', '..', '..', 'ext')

require ::File.join(ext_base, "jars/slf4j-api-1.7.25.jar")
require ::File.join(ext_base, "jars/slf4j-simple-1.7.25.jar")
require ::File.join(ext_base, "jars/gson-2.6.2.jar")
require ::File.join(ext_base, "jars/jnats-1.1-SNAPSHOT.jar")

module Protobuf
  module Nats
    class JNats
      attr_reader :connection

      class Message
        attr_reader :data, :subject, :reply

        def initialize(nats_message)
          @data = nats_message.getData.to_s
          @reply = nats_message.getReplyTo.to_s
          @subject = nats_message.getSubject
        end
      end

      def initialize
      end

      # def create_key_manager_factory(path, type)
      #   key_store_factory = javax.net.ssl.KeyManagerFactory.getInstance(type)
      #   path_input_stream = java.io.FileInputStream.new(path)
      #   key_store = java.security.KeyStore.getInstance(type)
      #   fail "JVM does not support a key manager factory for #{type}" if key_store.nil?
      #   key_store.load(path_input_stream, nil)
      #   key_store_factory.inti(key_store, nil)
      #   key_store_factory
      # end

      def connect(options = {})
        servers = options[:servers] || ["nats://localhost:4222"]
        servers = [servers].flatten.map { |uri_string| java.net.URI.new(uri_string) }
        connection_factory = ::Java::IoNatsClient::ConnectionFactory.new
        connection_factory.setServers(servers)
        # Basically never stop trying to connect
        connection_factory.setMaxReconnect(60_000)

        #  if options[:uses_tls]
        #   tls_client_cert_path = options[:tls_client_cert]
        #   tls_client_key_path = options[:tls_client_key]
        #   tls_ca_cert_path = options[:tls_ca_cert]
        #   tls_client_cert_is = java.io.FileInputStream.new(tls_client_cert_path)
        #   tls_client_key_is = java.io.FileInputStream.new(tls_client_key_path)
        #   tls_ca_cert_is = java.io.FileInputStream.new(tls_ca_cert_path)

        #   key_store_factory = javax.net.ssl.KeyManagerFactory.getInstance("X.509")
        #   fail "JVM does not support a key manager factory for X.509" if key_store.nil?
        #   cert_key_store.load(tls_client_cert_is, nil)
        #   cert_key_store.load(tls_client_key_is, nil)
        #   cert_key_store_factory.init(key_store, nil)


        #   trust_store_factory = javax.net.ssl.KeyManagerFactory.getInstance("X.509")
        #   fail "JVM does not support a trust manager factory for X.509" if trust_store.nil?
        #   trust_store.load(tls_client_cert_is, nil)
        #   trust_store.load(tls_client_trust_is, nil)
        #   trust_store_factory.init(trust_store, nil)





        #   key_store = java.security.KeyStore.getInstance("JKS")
        # end

        @connection ||= connection_factory.createConnection
      end

      def close
        @connection.close
      end

      def flush(timeout_sec = 0.5)
        @connection.flush(timeout_sec * 1000)
      end

      def next_message(sub, timeout_sec)
        nats_message = sub.nextMessage(timeout_sec * 1000)
        return nil unless nats_message
        Message.new(nats_message)
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
