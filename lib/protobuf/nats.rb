require "protobuf/nats/version"

require "protobuf"
# We don't need this, but the CLI attempts to terminate.
require "protobuf/rpc/service_directory"

require "nats/io/client"

require "protobuf/nats/errors"
require "protobuf/nats/client"
require "protobuf/nats/server"
require "protobuf/nats/runner"
require "protobuf/nats/config"

module Protobuf
  module Nats
    class << self
      attr_accessor :client_nats_connection
    end

    module Messages
      ACK  = "\1".freeze
      NACK = "\2".freeze
    end

    NatsClient = if defined? JRUBY_VERSION
                   require "protobuf/nats/jnats"
                   ::Protobuf::Nats::JNats
                 else
                   ::NATS::IO::Client
                 end

    GET_CONNECTED_MUTEX = ::Mutex.new

    def self.config
      @config ||= begin
        config = ::Protobuf::Nats::Config.new
        config.load_from_yml
        config
      end
    end

    # Eagerly load the yml config.
    config

    # We will always log  an error.
    def self.error_callbacks
      @error_callbacks ||= [lambda { |error| log_error(error) }]
    end

    # Eagerly load the yml config.
    error_callbacks

    def self.on_error(&block)
      fail ::ArgumentError unless block.arity == 1
      error_callbacks << block
      nil
    end

    def self.notify_error_callbacks(error)
      error_callbacks.each do |callback|
        begin
          callback.call(error)
        rescue => callback_error
          log_error(callback_error)
        end
      end

      nil
    end

    def self.subscription_key(service_klass, service_method)
      service_class_name = service_klass.name.underscore.gsub("/", ".")
      service_method_name = service_method.to_s.underscore

      subscription_key = "rpc.#{service_class_name}.#{service_method_name}"
      subscription_key = config.make_subscription_key_replacements(subscription_key)
    end

    def self.start_client_nats_connection
      return true if @client_nats_connection

      GET_CONNECTED_MUTEX.synchronize do
        break true if @client_nats_connection

        # Disable publisher pending buffer on reconnect
        options = config.connection_options.merge(:disable_reconnect_buffer => true)

        client = NatsClient.new
        client.connect(options)

        # Ensure we have a valid connection to the NATS server.
        client.flush(5)

        client.on_disconnect do
          logger.warn("Client NATS connection was disconnected")
        end

        client.on_reconnect do
          logger.warn("Client NATS connection was reconnected")
        end

        client.on_close do
          logger.warn("Client NATS connection was closed")
        end

        client.on_error do |error|
          notify_error_callbacks(error)
        end

        @client_nats_connection = client

        true
      end
    end

    # This will work with both ruby and java errors
    def self.log_error(error)
      logger.error error.to_s
      logger.error error.class.to_s
      if error.respond_to?(:backtrace) && error.backtrace.is_a?(::Array)
        logger.error error.backtrace.join("\n")
      end
    end

    def self.logger
      ::Protobuf::Logging.logger
    end

    logger.info "Using #{NatsClient} to connect"

    at_exit do
      ::Protobuf::Nats.client_nats_connection.close rescue nil
    end

  end
end
