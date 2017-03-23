require "securerandom"
require "thread"

class FakeNatsClient
  Request = Struct.new(:subject, :data, :seconds_in_future)

  def initialize(options)
    @inbox = options[:inbox] || ::SecureRandom.uuid
    @subscriptions = {}
  end

  def new_inbox
    @inbox
  end

  def publish(*)
  end

  def subscribe(subject, args, &block)
    @subscriptions[subject] = block
  end

  def unsubscribe(*)
  end

  def schedule_request(request)
    schedule_requests([request])
  end

  def schedule_requests(requests)
    requests.each do |request|
      Thread.new do
        begin
          sleep request.seconds_in_future
          block = @subscriptions[request.subject]
          block.call(request.data) if block
        rescue => error
          puts error
        end
      end
    end
  end
end
