require "spec_helper"

describe ::Protobuf::Nats::Client do
  class ExampleServiceClass; end

  let(:service) { ExampleServiceClass }
  let(:method) { :created }
  let(:options) {
    {
      :service => service,
      :method => method
    }
  }

  subject { described_class.new(options) }

  describe "#cached_subscription_key" do
    it "caches the instance of a subscription key" do
      ::Protobuf::Nats::Client.instance_variable_set(:@subscription_key_cache, nil)
      id = subject.cached_subscription_key.__id__
      expect(subject.cached_subscription_key.__id__).to eq(id)
    end
  end

  describe "#nats_request_with_two_responses" do
    let(:client) { ::FakeNatsClient.new(:inbox => inbox) }
    let(:inbox) { "INBOX_123" }
    let(:msg_subject) { "rpc.yolo.brolo" }
    let(:ack) { ::Protobuf::Nats::Messages::ACK }
    let(:response) { "final count down" }

    before do
      allow(::Protobuf::Nats).to receive(:client_nats_connection).and_return(client)
    end

    it "processes a request and return the final response" do
      client.schedule_messages([::FakeNatsClient::Message.new(inbox, ack, 0.05),
                                ::FakeNatsClient::Message.new(inbox, response, 0.1)])

      server_response = subject.nats_request_with_two_responses(msg_subject, "request data", {})
      expect(server_response).to eq(response)
    end

    it "raises an error when the ack is not signaled" do
      client.schedule_messages([::FakeNatsClient::Message.new(inbox, response, 0.05)])

      options = {:ack_timeout => 0.1, :timeout => 0.2}
      expect { subject.nats_request_with_two_responses(msg_subject, "request data", options) }.to raise_error(::NATS::IO::Timeout)
    end

    it "can send messages out of order and still complete" do
      client.schedule_messages([::FakeNatsClient::Message.new(inbox, response, 0.05),
                                ::FakeNatsClient::Message.new(inbox, ack, 0.1)])

      server_response = subject.nats_request_with_two_responses(msg_subject, "request data", {})
      expect(server_response).to eq(response)
    end

    it "raises an error when the ack is signaled but pb response is not" do
      client.schedule_messages([::FakeNatsClient::Message.new(inbox, ack, 0.05)])

      options = {:timeout => 0.1}
      expect { subject.nats_request_with_two_responses(msg_subject, "request data", options) }.to raise_error(::NATS::IO::Timeout)
    end
  end

  describe "#send_request" do
    it "retries 3 times when a NAT timeout is raised" do
      expect(subject).to receive(:setup_connection).exactly(3).times
      expect(subject).to receive(:nats_request_with_two_responses).and_raise(::NATS::IO::Timeout).exactly(3).times
      expect { subject.send_request }.to raise_error(::NATS::IO::Timeout)
    end
  end
end
