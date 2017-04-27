require "spec_helper"

describe ::Protobuf::Nats do
  it "has a version number" do
    expect(Protobuf::Nats::VERSION).not_to be nil
  end

  class ExampleServiceKlassBro; end

  it "can generate a correct subscription key" do
    expect(described_class.subscription_key(ExampleServiceKlassBro, :yolo_dude)).to eq("rpc.example_service_klass_bro.yolo_dude")
  end

  describe "#on_error" do
    # Reset error callbacks.
    before { described_class.instance_variable_set(:@error_callbacks, nil) }
    after { described_class.instance_variable_set(:@error_callbacks, nil) }

    it "has a logger error handler" do
      expect(described_class.error_callbacks.size).to eq(1)
      expect(described_class).to receive(:log_error).with("test")
      described_class.notify_error_callbacks("test")
    end

    it "can have multiple error callbacks" do
      was_invoked = false
      described_class.on_error do |_error|
        was_invoked = true
      end
      expect(described_class.error_callbacks.size).to eq(2)
      described_class.notify_error_callbacks(::ArgumentError.new("test"))
      expect(was_invoked).to eq(true)
    end

    it "raises an argument error when a callback does not have arity of 1" do
      expect { described_class.on_error {} }.to raise_error(::ArgumentError)
    end
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

  describe "#notify_error_callbacks" do
    it "logs an error raised by a callback" do
      the_error = ::RuntimeError.new("Save me from my callback sadness!")
      error_callback = lambda { |_error| fail the_error }
      allow(described_class).to receive(:error_callbacks).and_return([error_callback])

      expect(described_class).to receive(:log_error).with(the_error)
      described_class.notify_error_callbacks("yolo")
    end
  end
end
