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

      class MriIllegalStateException < ::StandardError
      end

      IllegalStateException = if defined? JRUBY_VERSION
                                java.lang.IllegalStateException
                              else
                                MriIllegalStateException
                              end

      IOException = if defined? JRUBY_VERSION
                      java.io.IOException
                    else
                      MriIOException
                    end
    end
  end
end
