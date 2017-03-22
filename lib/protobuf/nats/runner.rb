require "ostruct"
require "thread"

require "protobuf/nats"

module Protobuf
  module Nats
    class Runner

      def initialize(options)
        @options = case
                   when options.is_a?(OpenStruct) then
                     options.marshal_dump
                   when options.respond_to?(:to_hash) then
                     options.to_hash.symbolize_keys
                   else
                     fail "Cannot parse Nats Server - server options"
                   end
      end

      def run
        if ::Protobuf::Nats.ffi?
          @server = ::Protobuf::Nats::FfiServer.new(@options)
        else
          @server = ::Protobuf::Nats::Server.new(@options)
        end

        register_signals
        @server.run do
          yield if block_given?
        end
      end

      def running?
        @server.try :running?
      end

      def stop
        @server.try :stop
      end

      private

      def register_signals
        trap(:TRAP) do
          ::Thread.list.each do |thread|
            logger.info do
              <<-THREAD_TRACE
                #{thread.inspect}:
                  #{thread.backtrace.try(:join, $INPUT_RECORD_SEPARATOR)}"
              THREAD_TRACE
            end
          end
        end

      end
    end

  end
end
