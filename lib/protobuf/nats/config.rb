require "openssl"
require "yaml"

module Protobuf
  module Nats
    class Config
      attr_accessor :uses_tls, :servers, :connect_timeout, :tls_client_cert, :tls_client_key, :tls_ca_cert

      CONFIG_MUTEX = ::Mutex.new

      DEFAULTS = {
        :connect_timeout => nil,
        :servers => ["nats://localhost:4222"],
        :tls_client_cert => nil,
        :tls_client_key => nil,
        :tls_ca_cert => nil,
        :uses_tls => false,
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

            true
          end
        end
      end

    end
  end
end
