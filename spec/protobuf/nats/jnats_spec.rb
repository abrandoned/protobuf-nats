require "rspec"

if defined?(JRUBY_VERSION)
  require "protobuf/nats/jnats"

  describe ::Protobuf::Nats::JNats do
    describe "#connection" do
      it "calls #connect when no @connection exists" do
        expect(subject).to receive(:connect).with({})
        subject.connection
      end

      it "attempts to reconnect with options given to #connect" do
        allow(::Java::IoNatsClient::Nats).to receive(:connect).and_raise(::RuntimeError)
        provided_options = {:yolo => "ok"}
        subject.connect(provided_options) rescue nil
        expect(subject.options).to eq(provided_options)

        expect(subject).to receive(:connect).with(provided_options)
        subject.connection rescue nil
      end
    end
  end
end
