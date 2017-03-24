require "spec_helper"

describe ::Protobuf::Nats::FixedThreadPoolWithNoQueue do
  subject { described_class.new(5) }

  it "only allows work to be completed if threads are available" do
    task = lambda { sleep 1 }
    5.times { subject << task }
    expect { subject << task }.to raise_error(::Concurrent::RejectedExecutionError)
  end
end
