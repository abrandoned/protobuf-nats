FFI=1 PB_SERVER_TYPE="protobuf/nats/runner" PB_CLIENT_TYPE="protobuf/nats/client" bundle exec rpc_server start --threads=20 ./examples/warehouse/app.rb > /dev/null
