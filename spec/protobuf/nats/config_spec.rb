require "spec_helper"

describe ::Protobuf::Nats::Config do
  it "sets servers to localhost by default" do
    expect(subject.servers).to eq(["nats://localhost:4222"])
  end

  it "sets connect_timeout to nil by default" do
    expect(subject.connect_timeout).to eq(nil)
  end

  it "can load a custom cert, key and ca from yml" do
    ENV["PROTOBUF_NATS_CONFIG_PATH"] = "spec/support/protobuf_nats.yml"

    subject.load_from_yml
    expect(subject.tls_client_cert).to eq("./spec/support/certs/client-cert.pem")
    expect(subject.tls_client_key).to eq("./spec/support/certs/client-key.pem")
    expect(subject.tls_ca_cert).to eq("./spec/support/certs/ca.pem")

    ENV["PROTOBUF_NATS_CONFIG_PATH"] = nil
  end

  it "can load the yml from a specific directory" do
    ENV["PROTOBUF_NATS_CONFIG_PATH"] = "spec/support/protobuf_nats.yml"

    subject.load_from_yml
    expect(subject.servers).to eq(["nats://127.0.0.1:4222", "nats://127.0.0.1:4223", "nats://127.0.0.1:4224"])
    expect(subject.connect_timeout).to eq(2)

    ENV["PROTOBUF_NATS_CONFIG_PATH"] = nil
  end

  it "loads the defaults when a yml config is missing" do
    ENV["PROTOBUF_NATS_CONFIG_PATH"] = "spec/support/i_do_not_exist_because_im_not_real"

    subject.load_from_yml
    expect(subject.servers).to eq(["nats://localhost:4222"])
    expect(subject.uses_tls).to eq(false)
    expect(subject.tls_client_cert).to eq(nil)
    expect(subject.tls_client_key).to eq(nil)
    expect(subject.tls_ca_cert).to eq(nil)
    expect(subject.connect_timeout).to eq(nil)

    ENV["PROTOBUF_NATS_CONFIG_PATH"] = nil
  end
end
