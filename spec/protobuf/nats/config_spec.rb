require "spec_helper"

describe ::Protobuf::Nats::Config do
  it "sets servers and connect_timeout to nil by default" do
    expect(subject.servers).to eq(nil)
    expect(subject.connect_timeout).to eq(nil)
  end

  it "does not load tls by default" do
    subject.servers = ["nats://127.0.0.1:4222"]
    expected_options = {
      :servers => ["nats://127.0.0.1:4222"],
      :connect_timeout => nil
    }
    expect(subject.connection_options).to eq(expected_options)
  end

  it "can provide a tls context" do
    subject.servers = ["nats://127.0.0.1:4222"]
    subject.uses_tls = true
    tls_context = subject.connection_options[:tls][:context]
    expect(tls_context).to be_an(::OpenSSL::SSL::SSLContext)
  end

  it "can load a custom cert into the ssl context" do
    ENV["PROTOBUF_NATS_CONFIG_PATH"] = "spec/support/protobuf_nats.yml"

    subject.load_from_yml
    expected_cert = ::File.read("spec/support/certs/client-cert.pem")
    expect(subject.new_tls_context.cert.to_s).to eq(expected_cert)

    ENV["PROTOBUF_NATS_CONFIG_PATH"] = nil
  end

  it "can load a custom key into the ssl context" do
    ENV["PROTOBUF_NATS_CONFIG_PATH"] = "spec/support/protobuf_nats.yml"

    subject.load_from_yml
    expected_key = ::File.read("spec/support/certs/client-key.pem")
    expect(subject.new_tls_context.key.to_s).to eq(expected_key)

    ENV["PROTOBUF_NATS_CONFIG_PATH"] = nil
  end

  it "can load the yml from a specific directory" do
    ENV["PROTOBUF_NATS_CONFIG_PATH"] = "spec/support/protobuf_nats.yml"

    subject.load_from_yml
    expect(subject.servers).to eq(["nats://127.0.0.1:4222", "nats://127.0.0.1:4223", "nats://127.0.0.1:4224"])
    expect(subject.uses_tls).to eq(true)
    expect(subject.connect_timeout).to eq(2)

    ENV["PROTOBUF_NATS_CONFIG_PATH"] = nil
  end

  it "loads the defaults when a yml config is missing" do
    ENV["PROTOBUF_NATS_CONFIG_PATH"] = "spec/support/i_do_not_exist_because_im_not_real"

    subject.load_from_yml
    expect(subject.servers).to eq(nil)
    expect(subject.uses_tls).to eq(false)
    expect(subject.connect_timeout).to eq(nil)

    ENV["PROTOBUF_NATS_CONFIG_PATH"] = nil
  end

  describe "#ack_timeout_in_seconds" do
    it "uses the default when no env is specified" do
      expect(::Protobuf::Nats::Config::DEFAULT_ACK_TIMEOUT_IN_SECONDS).to eq(5.0)
      expect(subject.ack_timeout_in_seconds).to eq(::Protobuf::Nats::Config::DEFAULT_ACK_TIMEOUT_IN_SECONDS)
    end

    it "can be set with an env variable" do
      ENV["PB_NATS_ACK_TIMEOUT_IN_SECONDS"] = "0.5"
      expect(subject.ack_timeout_in_seconds).to eq(0.5)
      ENV.delete("PB_NATS_ACK_TIMEOUT_IN_SECONDS")
    end
  end

  describe "#response_timeout_in_seconds" do
    it "uses the default when no env is specified" do
      expect(::Protobuf::Nats::Config::DEFAULT_RESPONSE_TIMEOUT_IN_SECONDS).to eq(60.0)
      expect(subject.response_timeout_in_seconds).to eq(::Protobuf::Nats::Config::DEFAULT_RESPONSE_TIMEOUT_IN_SECONDS)
    end

    it "can be set with an env variable" do
      ENV["PB_NATS_RESPONSE_TIMEOUT_IN_SECONDS"] = "1.5"
      expect(subject.response_timeout_in_seconds).to eq(1.5)
      ENV.delete("PB_NATS_RESPONSE_TIMEOUT_IN_SECONDS")
    end
  end
end
