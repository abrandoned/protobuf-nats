require "protobuf/nats/version"

require "protobuf"
# We don't need this, but the CLI attempts to terminate.
require "protobuf/rpc/service_directory"

require "nats/io/client"

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
      ACK = "\1".freeze
    end

    NatsClient = if defined? JRUBY_VERSION
                   require "protobuf/nats/jnats"
                   ::Protobuf::Nats::JNats
                 else
                   ::NATS::IO::Client
                 end

    puts "Using #{NatsClient} to connect"

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

    def self.subscription_key(service_klass, service_method)
      service_class_name = service_klass.name.underscore.gsub("/", ".")
      service_method_name = service_method.to_s.underscore
      "rpc.#{service_class_name}.#{service_method_name}"
    end

    def self.start_client_nats_connection
      @start_client_nats_connection ||= begin
        GET_CONNECTED_MUTEX.synchronize do
          return if @start_client_nats_connection

          @client_nats_connection = NatsClient.new
          @client_nats_connection.connect(config.connection_options)

          # Ensure we have a valid connection to the NATS server.
          @client_nats_connection.flush(5)

          @client_nats_connection.on_error do |error|
            log_error(error)
          end

          true
        end
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

    at_exit do
      ::Protobuf::Nats.client_nats_connection.close rescue nil
    end

  end
end
