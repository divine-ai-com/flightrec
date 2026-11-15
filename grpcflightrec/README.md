# grpcflightrec

gRPC server and client interceptors for [flightrec](https://github.com/divine-ai-com/flightrec).

```bash
go get github.com/divine-ai-com/flightrec/grpcflightrec
```

Server interceptors open a scope per RPC, take the trace ID from the request's OpenTelemetry trace if it has one or else from incoming metadata (`traceparent`, `x-trace-id`, `x-request-id`), and set the `flightrec-flush` trailer when the RPC is kept. Client interceptors read that trailer, as `NewTransport` does for HTTP (see [Keep logs across services](https://github.com/divine-ai-com/flightrec/blob/main/docs/cross-service.md)):

```go
import "github.com/divine-ai-com/flightrec/grpcflightrec"

srv := grpc.NewServer(
	grpc.UnaryInterceptor(grpcflightrec.UnaryServerInterceptor(rec)),
	grpc.StreamInterceptor(grpcflightrec.StreamServerInterceptor(rec)),
)

conn, err := grpc.NewClient(target,
	grpc.WithUnaryInterceptor(grpcflightrec.UnaryClientInterceptor()),
	grpc.WithStreamInterceptor(grpcflightrec.StreamClientInterceptor()),
	// credentials and other options
)
```

Or use `flightrec.InvokeGRPCUnary` and `flightrec.InvokeGRPCStream` directly from the zero-dependency root module.

The server interceptors flush and then re-panic when a handler panics, as `HTTPMiddleware` does. gRPC servers do not recover panics, so chain a recovery interceptor outside flightrec's (`grpc.ChainUnaryInterceptor(recovery, grpcflightrec.UnaryServerInterceptor(rec))`), or a panic still stops the server.
