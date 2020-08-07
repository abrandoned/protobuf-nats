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
        allow(::Java::IoNatsClient::ConnectionFactory).to receive(:new).and_raise(::RuntimeError)
        provided_options = {:yolo => "ok"}
        subject.connect(provided_options) rescue nil
        expect(subject.options).to eq(provided_options)

        expect(subject).to receive(:connect).with(provided_options)
        subject.connection rescue nil
      end
    end

    describe "#connect" do
      it "creates a new message channel" do
        subject.connect
        subject.close
      end
    end

    context "integration tests" do
      context "async subscribe" do
        before { subject.connect }
        after { subject.close rescue nil }

        it "can subscribe async and receive a message" do
          # Set up an async receiver.
          server_sub = subject.subscribe("yolo.brolo", :queue => "yolo.brolo") do |request, reply_id, _subject|
            expect(request).to eq("hello")
            subject.publish(reply_id, "received")
            subject.flush
          end

          # Set up a blocking subscription for the reply.
          client_sub = subject.subscribe("hit.me.back")

          # Send a message to the server.
          subject.publish("yolo.brolo", "hello", "hit.me.back")
          subject.flush

          # Use client to wait for response.
          response = subject.next_message(client_sub, 1)
          expect(response.data).to eq("received")

          # Clean up
          subject.unsubscribe(client_sub)
          subject.unsubscribe(server_sub)
        end
      end
    end

    context "auto unsubscribe" do
      before { subject.connect }
      after { subject.close rescue nil }

      it "can auto unsub after n messages" do
        sub = subject.subscribe("hey.dude", :max => 2)

        expect(sub.isClosed).to eq(false)

        # First message
        subject.publish("hey.dude", "message1")
        response = subject.next_message(sub, 1)
        expect(response.data).to eq("message1")

        # Second message
        subject.publish("hey.dude", "message2")
        response = subject.next_message(sub, 1)
        expect(response.data).to eq("message2")

        # All done
        expect(sub.isClosed).to eq(true)
      end
    end
  end
end
