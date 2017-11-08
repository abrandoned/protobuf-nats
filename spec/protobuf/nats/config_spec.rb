require "spec_helper"

describe ::Protobuf::Nats::Config do
  it "sets servers and connect_timeout to nil by default" do
    expect(subject.servers).to eq(nil)
    expect(subject.connect_timeout).to eq(nil)
  end

  it "has default options without tls" do
    subject.servers = ["nats://127.0.0.1:4222"]
    expected_options = {
      :servers => ["nats://127.0.0.1:4222"],
      :connect_timeout => nil,
      :tls_ca_cert => nil,
      :tls_client_cert => nil,
      :tls_client_key => nil,
      :uses_tls => false,
      :max_reconnect_attempts => 60_000,
      :server_subscription_key_do_not_subscribe_to_when_includes_any_of => [],
      :server_subscription_key_only_subscribe_to_when_includes_any_of => [],
      :subscription_key_replacements => [],
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
    expect(subject.max_reconnect_attempts).to eq(1234)
    expect(subject.subscription_key_replacements).to eq([{"original_" => "local_"}, {"another_subscription" => "different_subscription"}])
    expect(subject.server_subscription_key_only_subscribe_to_when_includes_any_of).to eq(["search", "derp"])
    expect(subject.server_subscription_key_do_not_subscribe_to_when_includes_any_of).to eq(["derpderp", "searchsearch"])

    ENV["PROTOBUF_NATS_CONFIG_PATH"] = nil
  end

  it "loads the defaults when a yml config is missing" do
    ENV["PROTOBUF_NATS_CONFIG_PATH"] = "spec/support/i_do_not_exist_because_im_not_real"

    subject.load_from_yml
    expect(subject.servers).to eq(nil)
    expect(subject.uses_tls).to eq(false)
    expect(subject.connect_timeout).to eq(nil)
    expect(subject.max_reconnect_attempts).to eq(60_000)

    ENV["PROTOBUF_NATS_CONFIG_PATH"] = nil
  end

  it "adds the tls options to the connection options" do
    ENV["PROTOBUF_NATS_CONFIG_PATH"] = "spec/support/protobuf_nats.yml"

    subject.load_from_yml
    connection_options = subject.connection_options
    expect(connection_options[:tls_client_cert]).to eq("./spec/support/certs/client-cert.pem")
    expect(connection_options[:tls_client_key]).to eq("./spec/support/certs/client-key.pem")
    expect(connection_options[:tls_ca_cert]).to eq("./spec/support/certs/ca.pem")

    ENV["PROTOBUF_NATS_CONFIG_PATH"] = nil
  end

  it "replaces subscription_key using subscription_key_replacements" do
    ENV["PROTOBUF_NATS_CONFIG_PATH"] = "spec/support/protobuf_nats.yml"

    subject.load_from_yml

    expect(subject.make_subscription_key_replacements("rpc.original_subscription")).to eq "rpc.local_subscription"
    expect(subject.make_subscription_key_replacements("rpc.another_subscription")).to eq "rpc.different_subscription"
    expect(subject.make_subscription_key_replacements("rpc.subscription")).to eq "rpc.subscription"
  end
end
