module github.com/haawklabs/flightrec/examples/cross-service

go 1.26.0

require (
	github.com/haawklabs/flightrec v0.9.0
	github.com/haawklabs/flightrec/notifier/redisnotifier v0.5.0
	github.com/redis/go-redis/v9 v9.23.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	go.uber.org/atomic v1.12.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
)

replace github.com/haawklabs/flightrec/notifier/redisnotifier => ../../notifier/redisnotifier

replace github.com/haawklabs/flightrec => ../..
