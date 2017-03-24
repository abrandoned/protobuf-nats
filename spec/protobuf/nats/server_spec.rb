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

  describe "execute_request_promise" do
    it "returns false when the thread pool is full" do
      # Block the thread pool.
      2.times { subject.thread_pool << lambda { sleep 1 } }
      expect(subject.execute_request_promise("", "")).to eq(nil)
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
