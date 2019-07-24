ext_base = ::File.join(::File.dirname(__FILE__), '..', '..', '..', 'ext')

require ::File.join(ext_base, "jars/slf4j-api-1.7.25.jar")
require ::File.join(ext_base, "jars/slf4j-simple-1.7.25.jar")
require ::File.join(ext_base, "jars/gson-2.6.2.jar")
require ::File.join(ext_base, "jars/jnats-2.6.0.jar")

module Protobuf
  module Nats
    class JNats
      attr_reader :connection, :dispatcher, :options

      class MessageHandlerProxy
        include ::Java::IoNatsClient::MessageHandler

        def self.empty
          new {}
        end

        def initialize(&block)
          @cb = block
        end

        def onMessage(message)
          @cb.call(message.getData.to_s, message.getReplyTo, message.getSubject)
        end
      end

      class ConnectionListener
        include ::Java::IoNatsClient::ConnectionListener

        def initialize
          @on_reconnect_cb = lambda {}
          @on_disconnect_cb = lambda {}
          @on_close_cb = lambda {}
        end

        def on_close(&block); @on_close_cb = block; end
        def on_disconnect(&block); @on_disconnect_cb = block; end
        def on_reconnect(&block); @on_reconnect_cb = block; end

        def connectionEvent(conn, event_type)
          case event_type
          when ::Java::IoNatsClient::ConnectionListener::Events::RECONNECTED
            @on_reconnect_cb.call
          when ::Java::IoNatsClient::ConnectionListener::Events::DISCONNECTED
            @on_disconnect_cb.call
          when ::Java::IoNatsClient::ConnectionListener::Events::CLOSED
            @on_close_cb.call
          end
        end
      end

      class ErrorListener
        include ::Java::IoNatsClient::ErrorListener

        def initialize
          @on_error_cb = lambda { |_error| }
          @on_exception_cb = lambda { |_exception| }
          @on_slow_consumer_cb = lambda { |_consumer| }
        end

        def on_error(&block)
          return if block.nil? || block.arity != 1
          @on_error_cb = block
        end

        def on_exception(&block)
          return if block.nil? || block.arity != 1
          @on_exception_cb = block
        end

        def on_slow_consumer(&block)
          return if block.nil? || block.arity != 1
          @on_slow_consumer_cb = block
        end

        def errorOccurred(_conn, error); @on_error_cb.call(error); end
        def exceptionOccurred(_conn, exception); @on_exception_cb.call(exception); end
        def slowConsumerDetected(_conn, consumer); @on_slow_consumer_cb.call(consumer); end
      end

      class Message
        attr_reader :data, :subject, :reply

        def initialize(nats_message)
          @data = nats_message.getData.to_s
          @reply = nats_message.getReplyTo.to_s
          @subject = nats_message.getSubject
        end
      end

      def initialize
        @connection_listener = ConnectionListener.new
        @error_listener = ErrorListener.new

        @options = nil
        @subz_cbs = {}
        @subz_mutex = ::Mutex.new
      end

      def connect(options = {})
        @options ||= options

        servers = options[:servers] || ["nats://localhost:4222"]
        servers = [servers].flatten

        builder = ::Java::IoNatsClient::Options::Builder.new
        builder.servers(servers)
        builder.maxReconnects(options[:max_reconnect_attempts])
        builder.errorListener(@error_listener)

        # Shrink the pending buffer to always raise an error and let the caller retry.
        if options[:disable_reconnect_buffer]
          builder.reconnectBufferSize(1)
        end

        # Setup ssl context if we're using tls
        if options[:uses_tls]
          ssl_context = create_ssl_context(options)
          builder.sslContext(ssl_context)
        end

        @connection = ::Java::IoNatsClient::Nats.connect(builder.build)
        @dispatcher = @connection.createDispatcher(MessageHandlerProxy.empty)
        @connection
      end

      def connection
        return @connection unless @connection.nil?
        # Ensure no consumer or supervisor are running
        close
        connect(options || {})
      end

      # Do not depend on #close for a graceful disconnect.
      def close
        if @connection
          @connection.closeDispatcher(@dispatcher) rescue nil
          @connection.close rescue nil
        end
        @dispatcher = nil
        @connection = nil
      end

      def flush(timeout_sec = 0.5)
        duration = duration_in_ms(timeout_sec * 1000)
        connection.flush(duration)
      end

      def next_message(sub, timeout_sec)
        duration = duration_in_ms(timeout_sec * 1000)
        nats_message = sub.nextMessage(duration)
        return nil unless nats_message
        Message.new(nats_message)
      end

      def publish(subject, data, mailbox = nil)
        # The "true" here is to force flush. May not need this.
        connection.publish(subject, mailbox, data.to_java_bytes)
        connection.flush(nil)
      end

      def subscribe(subject, options = {}, &block)
        queue = options[:queue]
        max = options[:max]

        if block
          handler = MessageHandlerProxy.new(&block)
          sub = subscribe_using_subscription_dispatcher(subject, queue, handler)
          # Auto unsub if max message option was provided.
          dispatcher.unsubscribe(sub, max) if max
          sub
        else
          sub = subscribe_using_connection(subject, queue)
          sub.unsubscribe(max) if max
          sub
        end
      end

      def unsubscribe(sub)
        return if sub.nil?
        if sub.getDispatcher
          dispatcher.unsubscribe(sub)
        else
          sub.unsubscribe()
        end
      end

      def new_inbox
        "_INBOX.#{::SecureRandom.hex(13)}"
      end

      def on_reconnect(&cb)
        @connection_listener.on_reconnect(&cb)
      end

      def on_disconnect(&cb)
        @connection_listener.on_disconnect(&cb)
      end

      def on_error(&cb)
        @error_listener.on_exception(&cb)
      end

      def on_close(&cb)
        @connection_listener.on_close(&cb)
      end

    private

      def duration_in_ms(ms); ::Java::JavaTime::Duration.ofMillis(ms); end

      def subscribe_using_connection(subject, queue)
        if queue
          connection.subscribe(subject, queue)
        else
          connection.subscribe(subject)
        end
      end

      def subscribe_using_subscription_dispatcher(subject, queue, handler)
        if queue
          dispatcher.java_send(:subscribe,
                               [::Java::JavaLang::String, ::Java::JavaLang::String, ::Java::IoNatsClient::MessageHandler],
                               subject, queue, handler)
        else
          dispatcher.java_send(:subscribe,
                               [::Java::JavaLang::String, ::Java::IoNatsClient::MessageHandler],
                               subject, handler)
        end
      end

      # Jruby-openssl depends on bouncycastle so our lives don't suck super bad
      def read_pem_object_from_file(path)
        fail ::ArgumentError, "Tried to read a PEM key or cert with path nil" if path.nil?

        file_reader = java.io.FileReader.new(path)
        pem_parser = org.bouncycastle.openssl.PEMParser.new(file_reader)
        object = pem_parser.readObject
        pem_parser.close
        object
      end

      def create_ssl_context(options)
        # Create our certs and key converters to go from bouncycastle to java.
        cert_converter = org.bouncycastle.cert.jcajce.JcaX509CertificateConverter.new
        key_converter = org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter.new

        # Load the certs and keys.
        tls_ca_cert = cert_converter.getCertificate(read_pem_object_from_file(options[:tls_ca_cert]))
        tls_client_cert = cert_converter.getCertificate(read_pem_object_from_file(options[:tls_client_cert]))
        tls_client_key = key_converter.getKeyPair(read_pem_object_from_file(options[:tls_client_key]))

        # Setup the CA cert.
        ca_key_store = java.security.KeyStore.getInstance(java.security.KeyStore.getDefaultType)
        ca_key_store.load(nil, nil)
        ca_key_store.setCertificateEntry("ca-certificate", tls_ca_cert)
        trust_manager = javax.net.ssl.TrustManagerFactory.getInstance(javax.net.ssl.TrustManagerFactory.getDefaultAlgorithm)
        trust_manager.init(ca_key_store)

        # Setup the cert / key pair.
        client_key_store = java.security.KeyStore.getInstance(java.security.KeyStore.getDefaultType)
        client_key_store.load(nil, nil)
        client_key_store.setCertificateEntry("certificate", tls_client_cert)
        certificate_java_array = [tls_client_cert].to_java(java.security.cert.Certificate)
        empty_password = [].to_java(:char)
        client_key_store.setKeyEntry("private-key", tls_client_key.getPrivate, empty_password, certificate_java_array)
        key_manager = javax.net.ssl.KeyManagerFactory.getInstance(javax.net.ssl.KeyManagerFactory.getDefaultAlgorithm)
        key_manager.init(client_key_store, empty_password)

        # Create ssl context.
        context = javax.net.ssl.SSLContext.getInstance("TLSv1.2")
        context.init(key_manager.getKeyManagers, trust_manager.getTrustManagers, nil)
        context
      end
    end
  end
end
