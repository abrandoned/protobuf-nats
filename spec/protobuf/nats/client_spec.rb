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

  describe "#ack_timeout" do
    it "can be set via the PB_NATS_CLIENT_ACK_TIMEOUT environment variable" do
      ::ENV["PB_NATS_CLIENT_ACK_TIMEOUT"] = "1000"

      expect(subject.ack_timeout).to eq(1_000)

      ::ENV.delete("PB_NATS_CLIENT_ACK_TIMEOUT")
    end

    it "has a default value" do
      expect(subject.ack_timeout).to eq(5)
    end
  end

  describe "#reconnect_delay" do
    it "can be set via the PB_NATS_CLIENT_RECONNECT_DELAY environment variable" do
      ::ENV["PB_NATS_CLIENT_RECONNECT_DELAY"] = "1000"

      expect(subject.reconnect_delay).to eq(1_000)

      ::ENV.delete("PB_NATS_CLIENT_RECONNECT_DELAY")
    end

    it "defaults to the ack_timeout" do
      expect(subject.reconnect_delay).to eq(subject.ack_timeout)
    end
  end

  describe "#response_timeout" do
    it "can be set via the PB_NATS_CLIENT_RESPONSE_TIMEOUT environment variable" do
      ::ENV["PB_NATS_CLIENT_RESPONSE_TIMEOUT"] = "1000"

      expect(subject.response_timeout).to eq(1_000)

      ::ENV.delete("PB_NATS_CLIENT_RESPONSE_TIMEOUT")
    end

    it "has a default value" do
      expect(subject.response_timeout).to eq(60)
    end
  end

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

    it "sets the server name when it is passed in the ack" do
      ack_with_server = "\x80\x00derpderp".force_encoding("BINARY")
      client.schedule_messages([::FakeNatsClient::Message.new(inbox, ack_with_server, 0.05),
                                ::FakeNatsClient::Message.new(inbox, response, 0.1)])

      server_response = subject.nats_request_with_two_responses(msg_subject, "request data", {})
      expect(server_response).to eq(response)
      expect(subject.instance_variable_get(:@stats).server).to eq("derpderp:0")
    end

    it "sets the server name when it is sent out of order" do
      ack_with_server = "\x80\x00derpderp".force_encoding("BINARY")
      client.schedule_messages([::FakeNatsClient::Message.new(inbox, ack_with_server, 0.1),
                                ::FakeNatsClient::Message.new(inbox, response, 0.05)])

      server_response = subject.nats_request_with_two_responses(msg_subject, "request data", {})
      expect(server_response).to eq(response)
      expect(subject.instance_variable_get(:@stats).server).to eq("derpderp:0")
    end
  end

  describe "#send_request" do
    it "retries 3 times when and raises a NATS timeout" do
      expect(subject).to receive(:setup_connection).exactly(3).times
      expect(subject).to receive(:nats_request_with_two_responses).and_raise(::NATS::IO::Timeout).exactly(3).times
      expect(subject).to receive(:complete).exactly(1).times
      expect { subject.send_request }.to raise_error(::NATS::IO::Timeout)
    end

    it "waits the reconnect_delay duration when the nats connection is reconnecting" do
      error = ::Protobuf::Nats::Errors::IOException.new
      client = ::FakeNatsClient.new
      allow(::Protobuf::Nats).to receive(:client_nats_connection).and_return(client)
      allow(client).to receive(:publish).and_raise(error)
      allow(subject).to receive(:setup_connection)
      expect(subject).to receive(:reconnect_delay).and_return(0.01).exactly(3).times
      expect { subject.send_request }.to raise_error(error)
    end
  end
end
