require "spec_helper"

describe ::Protobuf::Nats::Server do
  class SomeRandom < ::Protobuf::Message; end
  class SomeRandomService < ::Protobuf::Rpc::Service
    rpc :implemented, SomeRandom, SomeRandom
    rpc :not_implemented, SomeRandom, SomeRandom
    def implemented; end
  end

  let(:logger) { ::Logger.new(nil) }
  let(:client) { ::FakeNatsClient.new }
  let(:options) {
    {
      :threads => 2,
      :client  => client,
      :server  => 'derpaderp'
    }
  }

  subject { described_class.new(options) }

  before do
    allow(::Protobuf::Logging).to receive(:logger).and_return(logger)
    allow(subject).to receive(:service_klasses).and_return([SomeRandomService])
  end

  describe "#detect_and_handle_a_pause" do
    it "unsubscribes when the server is paused" do
      allow(subject).to receive(:paused?).and_return(true)
      expect(subject).to receive(:unsubscribe)
      subject.detect_and_handle_a_pause
    end

    it "subscribes and restarts slow start when the pause file is removed" do
      subject.instance_variable_set(:@processing_requests, false)
      expect(subject).to receive(:subscribe)
      subject.detect_and_handle_a_pause
    end

    it "never calls unsubscribe more than once per pause" do
      allow(subject).to receive(:paused?).and_return(true)
      expect(subject).to receive(:unsubscribe).once
      subject.detect_and_handle_a_pause
      subject.detect_and_handle_a_pause
      subject.detect_and_handle_a_pause
    end
    it "never calls subscribe more than once per pause" do
      subject.instance_variable_set(:@processing_requests, false)
      expect(subject).to receive(:subscribe).once
      subject.detect_and_handle_a_pause
      subject.detect_and_handle_a_pause
      subject.detect_and_handle_a_pause
    end
  end

  describe "#max_queue_size" do
    it "can be set via options hash" do
      expect(subject.max_queue_size).to eq(2)
    end

    it "can be set via PB_NATS_SERVER_MAX_QUEUE_SIZE environment variable" do
      ::ENV["PB_NATS_SERVER_MAX_QUEUE_SIZE"] = "10"

      expect(subject.max_queue_size).to eq(10)

      ::ENV.delete("PB_NATS_SERVER_MAX_QUEUE_SIZE")
    end
  end

  describe "pause_file_path" do
    it "is nil by default" do
      expect(subject.pause_file_path).to eq(nil)
    end

    it "can be set via PB_NATS_SERVER_PAUSE_FILE_PATH environment variable" do
      ::ENV["PB_NATS_SERVER_PAUSE_FILE_PATH"] = "/tmp/rpc-paused-bro"

      expect(subject.pause_file_path).to eq("/tmp/rpc-paused-bro")

      ::ENV.delete("PB_NATS_SERVER_PAUSE_FILE_PATH")
    end
  end

  describe "#paused?" do
    let(:test_file) { "#{::SecureRandom.uuid}-testing-123" }
    # Ensure the test file is always cleaned up.
    after { ::File.delete(test_file) if ::File.exist?(test_file) }

    it "pauses when a pause file is set" do
      ::ENV["PB_NATS_SERVER_PAUSE_FILE_PATH"] = test_file
      expect(subject).to_not be_paused
      ::File.write(test_file, "")
      expect(subject).to be_paused
      ::ENV.delete("PB_NATS_SERVER_PAUSE_FILE_PATH")
    end
  end

  describe "#slow_start_delay" do
    it "has a default" do
      expect(subject.slow_start_delay).to eq(10)
    end

    it "can be set via PB_NATS_SERVER_SLOW_START_DELAY environment variable" do
      ::ENV["PB_NATS_SERVER_SLOW_START_DELAY"] = "20"

      expect(subject.slow_start_delay).to eq(20)

      ::ENV.delete("PB_NATS_SERVER_SLOW_START_DELAY")
    end
  end

  describe "#subscriptions_per_rpc_endpoint" do
    it "has a default" do
      expect(subject.subscriptions_per_rpc_endpoint).to eq(10)
    end

    it "can be set via PB_NATS_SERVER_SUBSCRIPTIONS_PER_RPC_ENDPOINT environment variable" do
      ::ENV["PB_NATS_SERVER_SUBSCRIPTIONS_PER_RPC_ENDPOINT"] = "20"

      expect(subject.subscriptions_per_rpc_endpoint).to eq(20)

      ::ENV.delete("PB_NATS_SERVER_SUBSCRIPTIONS_PER_RPC_ENDPOINT")
    end
  end

  describe "#subscribe_to_services_once" do
    it "subscribes to services that inherit from protobuf rpc service" do
      subject.subscribe_to_services_once
      expect(client.subscriptions.keys).to eq(["rpc.some_random_service.implemented"])
    end
  end

  describe "#enqueue_request" do
    it "returns false when the thread pool and thread pool queue is full and publish NACK" do
      # Fill the thread pool.
      2.times { subject.thread_pool.push { sleep 1 } }
      # Fill the thread pool queue.
      2.times { subject.thread_pool.push { sleep 1 } }

      expect(subject.nats).to receive(:publish).with("inbox_123", ::Protobuf::Nats::Messages::NACK)
      expect(subject.enqueue_request("", "inbox_123")).to eq(false)
    end

    it "sends an ACK if the thread pool enqueued the task" do
      # Fill the thread pool.
      2.times { subject.thread_pool.push { sleep 1 } }
      expect(subject.nats).to receive(:publish).with("inbox_123", ::Protobuf::Nats::Messages::ACK)
      # Wait for promise to finish executing.
      expect(subject.enqueue_request("", "inbox_123")).to eq(true)
      subject.thread_pool.kill
    end

    it "logs any error that is raised within the request block" do
      request_data = "yolo"
      expect(subject).to receive(:handle_request).with(request_data, 'server' => 'derpaderp').and_raise(::RuntimeError, "mah error")
      expect(logger).to receive(:error).once.ordered.with("mah error")
      expect(logger).to receive(:error).once.ordered.with("RuntimeError")
      expect(logger).to receive(:error).once.ordered

      # Wait for promise to finish executing.
      expect(subject.enqueue_request(request_data, "inbox_123")).to eq(true)
      sleep 0.1 until subject.thread_pool.size.zero?
    end

    it "returns an ACK and a response" do
      response = "some response data"
      inbox = "inbox_123"
      expect(subject).to receive(:handle_request).and_return(response)
      expect(client).to receive(:publish).once.ordered.with(inbox, ::Protobuf::Nats::Messages::ACK)
      expect(client).to receive(:publish).once.ordered.with(inbox, response)

      # Wait for promise to finish executing.
      expect(subject.enqueue_request("", inbox)).to eq(true)
      sleep 0.1 until subject.thread_pool.size.zero?
    end
  end

  describe "instrumentation" do
    it "instruments the thread pool execution delay" do
      expect(subject).to receive(:handle_request).and_return("response")
      execution_delay = nil
      subscription = ::ActiveSupport::Notifications.subscribe "server.thread_pool_execution_delay.protobuf-nats" do |_, _, _, _, delay|
        execution_delay = delay
      end

      subject.enqueue_request("", "YOLO123")
      sleep 0.1 until subject.thread_pool.size.zero?

      expect(execution_delay).to be > 0
      ::ActiveSupport::Notifications.unsubscribe(subscription)
    end

    it "instrument a request duration" do
      expect(subject).to receive(:handle_request) do
        sleep 0.05
        "response"
      end
      request_duration = nil
      subscription = ::ActiveSupport::Notifications.subscribe "server.request_duration.protobuf-nats" do |_, _, _, _, duration|
        request_duration = duration
      end

      subject.enqueue_request("", "YOLO123")
      sleep 0.1 until subject.thread_pool.size.zero?

      expect(request_duration).to be > 0.05
      ::ActiveSupport::Notifications.unsubscribe(subscription)
    end

    it "instruments when a message received" do
      allow(subject.thread_pool).to receive(:push)
      message_was_received = false
      subscription = ::ActiveSupport::Notifications.subscribe "server.message_received.protobuf-nats" do
        message_was_received = true
      end

      subject.enqueue_request("", "YOLO123")
      sleep 0.1 until subject.thread_pool.size.zero?

      expect(message_was_received).to eq(true)
      ::ActiveSupport::Notifications.unsubscribe(subscription)
    end

    it "instruments when a message dropped" do
      allow(subject.thread_pool).to receive(:push).and_return(false)
      message_was_dropped = false
      subscription = ::ActiveSupport::Notifications.subscribe "server.message_dropped.protobuf-nats" do
        message_was_dropped = true
      end

      subject.enqueue_request("", "YOLO123")
      sleep 0.1 until subject.thread_pool.size.zero?

      expect(message_was_dropped).to eq(true)
      ::ActiveSupport::Notifications.unsubscribe(subscription)
    end
  end
end
