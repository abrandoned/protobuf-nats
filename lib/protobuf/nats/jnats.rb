ext_base = ::File.join(::File.dirname(__FILE__), '..', '..', '..', 'ext')

require ::File.join(ext_base, "jars/slf4j-api-1.7.25.jar")
require ::File.join(ext_base, "jars/slf4j-simple-1.7.25.jar")
require ::File.join(ext_base, "jars/gson-2.6.2.jar")
require ::File.join(ext_base, "jars/jnats-1.1-SNAPSHOT.jar")

# Set field accessors so we can access the member variables directly.
class Java::IoNatsClient::SubscriptionImpl
  field_accessor :pMsgs, :pBytes, :delivered
end

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
        @on_error_cb = lambda {|error|}
        @on_reconnect_cb = lambda {}
        @on_disconnect_cb = lambda {}
        @on_close_cb = lambda {}
        @subz_cbs = {}
        @subz_mutex = ::Mutex.new
      end

      def connect(options = {})
        servers = options[:servers] || ["nats://localhost:4222"]
        servers = [servers].flatten.map { |uri_string| java.net.URI.new(uri_string) }
        connection_factory = ::Java::IoNatsClient::ConnectionFactory.new
        connection_factory.setServers(servers)
        connection_factory.setMaxReconnect(options[:max_reconnect_attempts])

        # Shrink the pending buffer to always raise an error and let the caller retry.
        if options[:disable_reconnect_buffer]
          connection_factory.setReconnectBufSize(1)
        end

        # Setup callbacks
        connection_factory.setDisconnectedCallback { |event| @on_disconnect_cb.call }
        connection_factory.setReconnectedCallback { |_event| @on_reconnect_cb.call }
        connection_factory.setClosedCallback { |_event| @on_close_cb.call }
        connection_factory.setExceptionHandler { |error| @on_error_cb.call(error) }

        # Setup ssl context if we're using tls
        if options[:uses_tls]
          ssl_context = create_ssl_context(options)
          connection_factory.setSecure(true)
          connection_factory.setSSLContext(ssl_context)
        end

        @connection = connection_factory.createConnection

        # We're going to spawn a consumer and supervisor
        @work_queue = @connection.createMsgChannel
        spwan_supervisor_and_consumer

        @connection
      end

      # Do not depend on #close for a greaceful disconnect.
      def close
        @connection.close
        @connection = nil
        @supervisor.kill rescue nil
        @supervisor = nil
        @consumer.kill rescue nil
        @supervisor = nil
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
        work_queue = nil
        # We pass our work queue for processing async work because java nats
        # uses a cahced thread pool: 1 thread per async subscription.
        # Sync subs need their own queue so work is not processed async.
        work_queue = block.nil? ? @connection.createMsgChannel : @work_queue
        sub = @connection.subscribe(subject, queue, nil, work_queue)

        # Register the block callback. We only lock to save the callback.
        if block
          @subz_mutex.synchronize do
            @subz_cbs[sub.getSid] = block
          end
        end

        # Auto unsub if max message option was provided.
        sub.autoUnsubscribe(max) if max

        sub
      end

      def unsubscribe(sub)
        return if sub.nil?

        # Cleanup our async callback
        if @subz_cbs[sub.getSid]
          @subz_mutex.synchronize do
            @subz_cbs.delete(sub.getSid)
          end
        end

        # The "true" here is to ignore and invalid conn.
        sub.unsubscribe(true)
      end

      def new_inbox
        "_INBOX.#{::SecureRandom.hex(13)}"
      end

      def on_reconnect(&cb)
        @on_reconnect_cb = cb
      end

      def on_disconnect(&cb)
        @on_disconnect_cb = cb
      end

      def on_error(&cb)
        @on_error_cb = cb
      end

      def on_close(&cb)
        @on_close_cb = cb
      end

    private

      def spwan_supervisor_and_consumer
        spawn_consumer
        @supervisor = ::Thread.new do
          loop do
            begin
              sleep 1
              next if @consumer && @consumer.alive?
              # We need to recreate the consumer thread
              @consumer.kill if @consumer
              spawn_consumer
            rescue => error
              @on_error_cb.call(error)
            end
          end
        end
      end

      def spawn_consumer
        @consumer = ::Thread.new do
          loop do
            begin
              message = @work_queue.take
              next unless message
              sub = message.getSubscription

              # We have to update the subscription stats so we're not considered a slow consumer.
              begin
                sub.lock
                sub.pMsgs -= 1
                sub.pBytes -= message.getData.length if message.getData
                sub.delivered += 1 unless sub.isClosed
              ensure
                sub.unlock
              end

              # We don't need t
              callback = @subz_cbs[sub.getSid]
              next unless callback
              callback.call(message.getData.to_s, message.getReplyTo, message.getSubject)
            rescue => error
              @on_error_cb.call(error)
            end
          end
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
