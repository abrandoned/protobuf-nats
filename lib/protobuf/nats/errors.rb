module Protobuf
  module Nats
    module Errors
      class Base < ::StandardError
      end

      class RequestTimeout < Base
      end

      class ResponseTimeout < Base
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
