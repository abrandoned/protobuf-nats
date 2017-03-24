require "concurrent"

module Protobuf
  module Nats
    class FixedThreadPoolWithNoQueue < ::Concurrent::FixedThreadPool
      def ns_initialize(opts)
        min_length = opts.fetch(:min_threads, DEFAULT_MIN_POOL_SIZE).to_i
        max_length = opts.fetch(:max_threads, DEFAULT_MAX_POOL_SIZE).to_i
        idletime = opts.fetch(:idletime, DEFAULT_THREAD_IDLETIMEOUT).to_i
        @fallback_policy = opts.fetch(:fallback_policy, :abort)

        if ::Concurrent.on_jruby?
          super(opts)

          # We need to use a synchronous queue to ensure we only perform work
          # when a thread is available.
          queue = java.util.concurrent.SynchronousQueue.new
          @executor = java.util.concurrent.ThreadPoolExecutor.new(
            min_length, max_length,
            idletime, java.util.concurrent.TimeUnit::SECONDS,
            queue, FALLBACK_POLICY_CLASSES[@fallback_policy].new)
        else
          # MRI will take a max queue of -1 to mean no queue.
          opts[:max_queue] = -1
          super(opts)
        end

        @max_queue = -1
      end
    end
  end
end
