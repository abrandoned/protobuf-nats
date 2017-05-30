module Protobuf
  module Nats
    class ThreadPool

      def initialize(size, opts = {})
        @queue = ::Queue.new

        # Callbacks
        @error_cb = lambda {|_error|}

        # Synchronization
        @mutex = ::Mutex.new
        @cb_mutex = ::Mutex.new

        # Let's get this party started
        queue_size = opts[:max_queue].to_i || 0
        @max_size = size + queue_size
        @max_workers = size
        @shutting_down = false
        @workers = []
        supervise_workers
      end

      def full?
        @queue.size >= @max_size
      end

      # This method is not thread safe by design since our IO model is a single producer thread
      # with multiple consumer threads.
      def push(&work_cb)
        return false if full?
        return false if @shutting_down
        @queue << [:work, work_cb]
        supervise_workers
        true
      end

      # This method is not thread safe by design since our IO model is a single producer thread
      # with multiple consumer threads.
      def shutdown
        @shutting_down = true
        @max_workers.times { @queue << [:stop, nil] }
      end

      def kill
        @shutting_down = true
        @workers.map(&:kill)
      end

      # This method is not thread safe by design since our IO model is a single producer thread
      # with multiple consumer threads.
      def wait_for_termination(seconds = nil)
        started_at = ::Time.now
        loop do
          sleep 0.1
          break if seconds && (::Time.now - started_at) >= seconds
          break if @workers.empty?
          prune_dead_workers
        end
      end

      # This callback is executed in a thread safe manner.
      def on_error(&cb)
        @error_cb = cb
      end

      def size
        @queue.size
      end

    private

      def prune_dead_workers
        @workers = @workers.select(&:alive?)
      end

      def supervise_workers
        prune_dead_workers
        missing_worker_count = (@max_workers - @workers.size)
        missing_worker_count.times do
          @workers << spawn_worker
        end
      end

      def spawn_worker
        ::Thread.new do
          loop do
            type, cb = @queue.pop
            begin
              # Break if we're shutting down
              break if type == :stop
              # Perform work
              cb.call
              # Update stats
            rescue => error
              @cb_mutex.synchronize { @error_cb.call(error) }
            end
          end
        end
      end

    end
  end
end
