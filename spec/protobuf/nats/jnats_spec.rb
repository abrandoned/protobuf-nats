require "rspec"

if defined?(JRUBY_VERSION)
  require "protobuf/nats/jnats"

  describe ::Protobuf::Nats::JNats do
    describe "#subscribe and #publish" do
      before { subject.connection }
      after { subject.close }

      it "can async subscribe multiple times" do
        times_received = 0
        lock = Mutex.new
        subject.subscribe("yolo.123") do
          lock.synchronize { times_received += 1 }
        end
        subject.publish("yolo.123", "test")
        verify_expectation_within(1) do
          expect(times_received).to eq(1)
        end
      end

      it "can sync subscribe" do
        expected_data = ::SecureRandom.uuid
        sub = subject.subscribe("yolo.345")
        subject.publish("yolo.345", expected_data)
        msg = subject.next_message(sub, 0.1)
        expect(msg.data).to eq(expected_data)
      end
    end

    describe "#next_message" do
      before { subject.connection }
      after { subject.close }

      it "returns nil when a timeout has expired" do
        sub = subject.subscribe("yolo.345")
        msg = subject.next_message(sub, 0.1)
        expect(msg).to be_nil
      end
    end

    describe "#unsubscribe" do
      before { subject.connection }
      after { subject.close }

      it "can unsub from an async subscription" do
        # This verifies the dispatcher is called correctly.
        async_sub = subject.subscribe("yolo.abc.async") {}
        expect(async_sub.is_active).to eq(true)
        subject.unsubscribe(async_sub)
        expect(async_sub.is_active).to eq(false)
      end

      it "can unsub from a sync subscription" do
        # This verifies the dispatcher is NOT called.
        sub = subject.subscribe("yolo.abc.not_sync")
        expect(sub.is_active).to eq(true)
        subject.unsubscribe(sub)
        expect(sub.is_active).to eq(false)
      end
    end

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
