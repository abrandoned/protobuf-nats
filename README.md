Protobuf::Nats
==============

An rpc client and server library built using the `protobuf` gem and the NATS protocol.

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'protobuf-nats'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install protobuf-nats

## Configuring

The client and server are configured via environment variables defined in the `pure-ruby-nats` gem. However, there are a
few params which cannot be set: `servers`, `uses_tls` and `connect_timeout`, so those my be defined in a yml file.

The library will automatically look for a file with a relative path of `config/protobuf_nats.yml`, but you may override
this by specifying a different file via the `PROTOBUF_NATS_CONFIG_PATH` env variable.

An example config looks like this:
```
# Stored at config/protobuf_nats.yml
---
  production:
    servers:
      - "nats://127.0.0.1:4222"
      - "nats://127.0.0.1:4223"
      - "nats://127.0.0.1:4224"
    uses_tls: true
    tls_client_cert: "/path/to/client-cert.pem"
    tls_client_key: "/path/to/client-key.pem"
    connect_timeout: 2
```

## Usage

This library is designed to be an alternative transport implementation used by the `protobuf` gem. In order to make
`protobuf` use this library, you need to set the following env variable:

```
PB_SERVER_TYPE="protobuf/nats/runner"
PB_CLIENT_TYPE="protobuf/nats/client"
```

## Example

NOTE: For a more detailed example, look at the `warehouse` app in the `examples` directory of this project.

Here's a tl;dr example. You might have a protobuf definition and implementation like this:

```ruby
require "protobuf/nats"

class User < ::Protobuf::Message
  optional :int64, :id, 1
  optional :string, :username, 2
end

class UserService < ::Protobuf::Rpc::Service
  rpc :create, User, User

  def create
    respond_with User.new(:id => 123, :username => request.username)
  end
end
```

Let's assume we saved this in a file called `app.rb`

We can now start an rpc server using the protobuf-nats runner and client:

```
$ export PB_SERVER_TYPE="protobuf/nats/runner"
$ export PB_CLIENT_TYPE="protobuf/nats/client"
$ bundle exec rpc_server start ./app.rb
...
I, [2017-03-24T12:16:02.539930 #12512]  INFO -- : Creating subscriptions:
I, [2017-03-24T12:16:02.543927 #12512]  INFO -- :   - rpc.user_service.create
...
```

And we can start a client and begin communicating:

```
$ export PB_SERVER_TYPE="protobuf/nats/runner"
$ export PB_CLIENT_TYPE="protobuf/nats/client"
$ bundle exec irb -r ./app
irb(main):001:0> UserService.client.create(User.new(:username => "testing 123"))
=> #<User id=123 username="testing 123">
```

And we can see the message was sent to the server and the server replied with a user which now has an `id`.

If we were to add another service endpoint called `search` to the `UserService` but fail to define an instance method
`search`, then `protobuf-nats` will not subscribe to that route.

## Development

After checking out the repo, run `bin/setup` to install dependencies. Then, run `rake test` to run the tests. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`. To release a new version, update the version number in `version.rb`, and then run `bundle exec rake release`, which will create a git tag for the version, push git commits and tags, and push the `.gem` file to [rubygems.org](https://rubygems.org).

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/abrandoned/protobuf-nats.


## License

The gem is available as open source under the terms of the [MIT License](http://opensource.org/licenses/MIT).
