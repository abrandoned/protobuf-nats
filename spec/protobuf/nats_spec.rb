require "spec_helper"

describe ::Protobuf::Nats do
  it "has a version number" do
    expect(Protobuf::Nats::VERSION).not_to be nil
  end

  class ExampleServiceKlassBro; end

  it "can generate a correct subscription key" do
    expect(described_class.subscription_key(ExampleServiceKlassBro, :yolo_dude)).to eq("rpc.example_service_klass_bro.yolo_dude")
  end
end
