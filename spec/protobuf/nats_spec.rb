require "spec_helper"

describe ::Protobuf::Nats do
  it "has a version number" do
    expect(Protobuf::Nats::VERSION).not_to be nil
  end

  class ExampleServiceKlassBro; end

  it "can generate a correct subscription key" do
    expect(described_class.subscription_key(ExampleServiceKlassBro, :yolo_dude)).to eq("rpc.example_service_klass_bro.yolo_dude")
  end

  describe "#log_error" do
    let(:logger) { ::Logger.new(nil) }

    before do
      allow(::Protobuf::Logging).to receive(:logger).and_return(logger)
    end

    it "does not log an error with a backtrace" do
      expect(logger).to receive(:error).with("yolo")
      expect(logger).to_not receive(:error).with("")
      ::Protobuf::Nats.log_error(::ArgumentError.new("yolo"))
    end

    it "logs errors with backtrace" do
      error = ::ArgumentError.new("yolo")
      allow(error).to receive(:backtrace).and_return(["line 1", "line 2"])
      expect(logger).to receive(:error).with("yolo")
      expect(logger).to_not receive(:error).with("line 1\nline2")
      ::Protobuf::Nats.log_error(error)
    end
  end
end
