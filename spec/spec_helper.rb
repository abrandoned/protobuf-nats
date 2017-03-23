require "bundler/setup"
require "protobuf/nats"
require "fake_nats_client"

RSpec.configure do |config|
  # Enable flags like --only-failures and --next-failure
  config.example_status_persistence_file_path = ".rspec_status"

  config.expect_with :rspec do |c|
    c.syntax = :expect
  end

  config.before(:each) do
    allow(Protobuf::Nats).to receive(:ensure_client_nats_connection_was_started)
  end
end
