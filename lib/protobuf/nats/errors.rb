module Protobuf
  module Nats
    module Errors
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
