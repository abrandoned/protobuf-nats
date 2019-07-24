require "bundler/setup"
require "protobuf/nats"
require "fake_nats_client"
require "pry"

# Turn off protobuf logging.
::Protobuf::Logging.logger = ::Logger.new(nil)

RSpec.configure do |config|
  # Enable flags like --only-failures and --next-failure
  config.example_status_persistence_file_path = ".rspec_status"
  config.order = :random
  config.color = true

  config.expect_with :rspec do |c|
    c.syntax = :expect
  end

  config.before(:each) do
    allow(Protobuf::Nats).to receive(:start_client_nats_connection)
  end
end

def verify_expectation_within(number_of_seconds, check_every = 0.02)
  waiting_since = ::Time.now
  begin
    sleep check_every
    yield
  rescue RSpec::Expectations::ExpectationNotMetError => e
    if ::Time.now - waiting_since > number_of_seconds
      raise e
    else
      retry
    end
  end
end
