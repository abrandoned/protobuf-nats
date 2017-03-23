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

  describe "#cached_subscription_key" do
    it "caches the instance of a subscription key" do
      ::Protobuf::Nats::Client.instance_variable_set(:@subscription_key_cache, nil)
      id = subject.cached_subscription_key.__id__
      expect(subject.cached_subscription_key.__id__).to eq(id)
    end
  end
end
