require "openssl"
require "yaml"

module Protobuf
  module Nats
    class Config
      attr_accessor :uses_tls, :servers, :connect_timeout, :tls_client_cert, :tls_client_key

      CONFIG_MUTEX = ::Mutex.new

      DEFAULTS = {
        :connect_timeout => nil,
        :servers => nil,
        :tls_client_cert => nil,
        :tls_client_key => nil,
        :uses_tls => false,
      }.freeze

      DEFAULT_ACK_TIMEOUT_IN_SECONDS = 5.0
      DEFAULT_RESPONSE_TIMEOUT_IN_SECONDS = 60.0

      def initialize
        DEFAULTS.each_pair do |key, value|
          __send__("#{key}=", value)
        end
      end

      def ack_timeout_in_seconds
        @ack_timeout_in_seconds ||= ENV.fetch("PB_NATS_ACK_TIMEOUT_IN_SECONDS", DEFAULT_ACK_TIMEOUT_IN_SECONDS).to_f
      end

      def response_timeout_in_seconds
        @response_timeout_in_seconds ||= ENV.fetch("PB_NATS_RESPONSE_TIMEOUT_IN_SECONDS", DEFAULT_RESPONSE_TIMEOUT_IN_SECONDS).to_f
      end

      def load_from_yml(reload = false)
        CONFIG_MUTEX.synchronize do
          @load_from_yml = nil if reload
          @load_from_yml ||= begin
            env = ENV["RAILS_ENV"] || ENV["RACK_ENV"] || ENV["APP_ENV"] || "development"

            yaml_config = {}
            config_path = ENV["PROTOBUF_NATS_CONFIG_PATH"] || ::File.join("config", "protobuf_nats.yml")
            absolute_config_path = ::File.expand_path(config_path)
            if ::File.exists?(absolute_config_path)
              yaml_config = ::YAML.load_file(absolute_config_path)[env]
            end

            DEFAULTS.each_pair do |key, value|
              setting = yaml_config[key.to_s]
              __send__("#{key}=", setting) if setting
            end

            # Reload the connection options hash
            connection_options(true)

            true
          end
        end
      end

      def connection_options(reload = false)
        @connection_options = false if reload
        @connection_options ||= begin
          options = {
            servers: servers,
            connect_timeout: connect_timeout
          }
          options[:tls] = {:context => new_tls_context} if uses_tls
          options
        end
      end

      def new_tls_context
        tls_context = ::OpenSSL::SSL::SSLContext.new
        tls_context.ssl_version = :TLSv1_2
        tls_context.cert = ::OpenSSL::X509::Certificate.new(::File.read(tls_client_cert)) if tls_client_cert
        tls_context.key = ::OpenSSL::PKey::RSA.new(::File.read(tls_client_key)) if tls_client_key
        tls_context
      end

    end
  end
end
