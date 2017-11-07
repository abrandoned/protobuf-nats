require "openssl"
require "yaml"

module Protobuf
  module Nats
    class Config
      attr_accessor :uses_tls, :servers, :connect_timeout, :tls_client_cert, :tls_client_key, :tls_ca_cert, :max_reconnect_attempts
      attr_accessor :server_subscription_key_do_not_subscribe_to_when_includes_any_of,
                    :server_subscription_key_only_subscribe_to_when_includes_any_of,
                    :subscription_key_replacements

      CONFIG_MUTEX = ::Mutex.new

      DEFAULTS = {
        :connect_timeout => nil,
        :max_reconnect_attempts => 60_000,
        :servers => nil,
        :tls_client_cert => nil,
        :tls_client_key => nil,
        :tls_ca_cert => nil,
        :uses_tls => false,
        :server_subscription_key_do_not_subscribe_to_when_includes_any_of => [],
        :server_subscription_key_only_subscribe_to_when_includes_any_of => [],
        :subscription_key_replacements => [],
      }.freeze

      def initialize
        DEFAULTS.each_pair do |key, value|
          __send__("#{key}=", value)
        end
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
            max_reconnect_attempts: max_reconnect_attempts,
            uses_tls: uses_tls,
            tls_client_cert: tls_client_cert,
            tls_client_key: tls_client_key,
            tls_ca_cert: tls_ca_cert,
            connect_timeout: connect_timeout,
            server_subscription_key_do_not_subscribe_to_when_includes_any_of: server_subscription_key_do_not_subscribe_to_when_includes_any_of,
            server_subscription_key_only_subscribe_to_when_includes_any_of: server_subscription_key_only_subscribe_to_when_includes_any_of,
            subscription_key_replacements: subscription_key_replacements,
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

      def make_subscription_key_replacements(subscription_key)
        subscription_key_replacements.each do |replacement|
          match = replacement.keys.first
          replacement = replacement[match]

          if subscription_key.include?(match)
            return subscription_key.gsub(match, replacement)
          end
        end

        subscription_key
      end
    end
  end
end
