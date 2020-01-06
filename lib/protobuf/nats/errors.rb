module Protobuf
  module Nats
    module Errors
      class ClientError < ::StandardError
      end

      class RequestTimeout < ClientError
      end

      class ResponseTimeout < ClientError
      end

      class MriIOException < ::StandardError
      end

      IOException = if defined? JRUBY_VERSION
                      java.io.IOException
                    else
                      MriIOException
                    end
    end
  end
end
