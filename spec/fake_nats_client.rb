require "securerandom"
require "thread"

class FakeNatsClient
  Message = Struct.new(:subject, :data, :seconds_in_future)

  attr_reader :subscriptions

  def initialize(options = {})
    @inbox = options[:inbox] || ::SecureRandom.uuid
    @subscriptions = {}
  end

  def connect(*)
  end

  def new_inbox
    @inbox
  end

  def publish(*)
  end

  def flush
  end

  def subscribe(subject, args, &block)
    subscriptions[subject] = block
  end

  def unsubscribe(*)
  end

  def next_message(_sub, timeout)
    timeout = timeout / 1_000.to_f
    started_at = ::Time.now
    @next_message = nil
    sleep 0.01 while @next_message.nil? && timeout > (::Time.now - started_at)
    @next_message
  end

  def schedule_message(message)
    schedule_messages([message])
  end

  def schedule_messages(messages)
    messages.each do |message|
      Thread.new do
        begin
          sleep message.seconds_in_future
          block = subscriptions[message.subject]
          block.call(message.data) if block
          @next_message = message
        rescue => error
          puts error
        end
      end
    end
  end
end
