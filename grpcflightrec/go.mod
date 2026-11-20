module github.com/haawklabs/flightrec/grpcflightrec

go 1.26.0

require (
	github.com/haawklabs/flightrec v0.9.0
	google.golang.org/grpc v1.83.2
	google.golang.org/protobuf v1.36.11
)

require (
	golang.org/x/net v0.62.0 // indirect
	golang.org/x/sys v0.49.0 // indirect
	golang.org/x/text v0.43.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260526163538-3dc84a4a5aaa // indirect
)

// Require core v0.1.0, which lacks APIs these versions call, so they do not compile.

replace github.com/haawklabs/flightrec => ../
