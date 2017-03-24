require "securerandom"
require "thread"

class FakeNatsClient
  Message = Struct.new(:subject, :data, :seconds_in_future)

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

  def schedule_message(message)
    schedule_messages([message])
  end

  def schedule_messages(messages)
    messages.each do |message|
      Thread.new do
        begin
          sleep message.seconds_in_future
          block = @subscriptions[message.subject]
          block.call(message.data) if block
        rescue => error
          puts error
        end
      end
    end
  end
end
