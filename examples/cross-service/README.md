# Cross-service flush example

Three services handle a checkout:

```
gateway  ->  orders  ->  inventory
```

When inventory fails, flightrec keeps the debug logs of all three, not only inventory's:

- **Synchronous failure** (`?fail=sync`): inventory answers 503. orders handles it and answers 200, and gateway answers 200. The `Flightrec-Flush` response header carries the failure back up the chain, so orders and gateway write their logs with reason `downstream:http_503`.
- **Asynchronous failure** (`?fail=async`): orders answers at once and reserves stock in the background. By the time inventory fails, gateway and orders have finished and kept their buffers (`Options.Retain`). The notifier announces the failure, and they write the logs they kept with reason `remote:error_log`.

## Run it in one process

```bash
go run .
```

The three services listen on random local ports and share an in-memory notifier. The output shows what each service wrote, prefixed with its name.

## Run it as separate processes with Redis

```bash
docker compose up --build
curl -s localhost:8081/checkout?fail=sync
curl -s localhost:8081/checkout?fail=async
docker compose logs gateway orders inventory
```

Or run each role yourself against a local Redis; see the comment at the top of `main.go`.

## What to look at in the code

- `newService`: each service creates a `Recorder` with `Retain` and wraps its HTTP client with `flightrec.NewTransport`.
- `call`: outgoing requests carry the trace ID in `X-Request-Id`, so every service files its logs under the same trace.
- `orders`: the background reservation opens its own scope with the request's trace ID.
- `main` and `runAll`: `StartNotifier` connects each Recorder to Redis or to the in-memory bus.
