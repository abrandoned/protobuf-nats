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
      :client => client
    }
  }

  subject { described_class.new(options) }

  before do
    allow(::Protobuf::Logging).to receive(:logger).and_return(logger)
    allow(subject).to receive(:service_klasses).and_return([SomeRandomService])
  end

  describe "#subscribe_to_services" do
    it "subscribes to services that inherit from protobuf rpc service" do
      subject.subscribe_to_services
      expect(client.subscriptions.keys).to eq(["rpc.some_random_service.implemented"])
    end
  end

  describe "#log_error" do
    it "does not log an error with a backtrace" do
      expect(subject.logger).to receive(:error).with("yolo")
      expect(subject.logger).to_not receive(:error).with("")
      subject.log_error(::ArgumentError.new("yolo"))
    end

    it "logs errors with backtrace" do
      error = ::ArgumentError.new("yolo")
      allow(error).to receive(:backtrace).and_return(["line 1", "line 2"])
      expect(subject.logger).to receive(:error).with("yolo")
      expect(subject.logger).to_not receive(:error).with("line 1\nline2")
      subject.log_error(error)
    end
  end

  describe "execute_request_promise" do
    it "returns nil when the thread pool and thread pool queue is full" do
      # Fill the thread pool.
      2.times { subject.thread_pool << lambda { sleep 1 } }
      # Fill the thread pool queue.
      2.times { subject.thread_pool << lambda { sleep 1 } }
      expect(subject.execute_request_promise("", "")).to eq(nil)
    end

    it "sends an ACK if the thread pool enqueued the task" do
      # Fill the thread pool.
      2.times { subject.thread_pool << lambda { sleep 1 } }
      expect(subject.nats).to receive(:publish).with("inbox_123", ::Protobuf::Nats::Messages::ACK)
      # Wait for promise to finish executing.
      promise = subject.execute_request_promise("", "inbox_123")
      subject.thread_pool.kill
    end

    it "logs any error that is raised within the request promise chain" do
      request_data = "yolo"
      expect(subject).to receive(:handle_request).with(request_data).and_raise(::RuntimeError)
      expect(logger).to receive(:error).once.ordered.with("RuntimeError")
      expect(logger).to receive(:error).once.ordered

      # Wait for promise to finish executing.
      promise = subject.execute_request_promise(request_data, "inbox_123")
      promise.value
    end

    it "returns an ACK and a response" do
      response = "some response data"
      inbox = "inbox_123"
      expect(subject).to receive(:handle_request).and_return(response)
      expect(client).to receive(:publish).once.ordered.with(inbox, ::Protobuf::Nats::Messages::ACK)
      expect(client).to receive(:publish).once.ordered.with(inbox, response)

      # Wait for promise to finish executing.
      promise = subject.execute_request_promise("", inbox)
      promise.value
    end
  end
end
