module github.com/divine-ai-com/flightrec/examples/cross-service

go 1.26.0

require (
	github.com/divine-ai-com/flightrec v0.3.0
	github.com/divine-ai-com/flightrec/notifier/redisnotifier v0.3.0
	github.com/redis/go-redis/v9 v9.23.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	go.uber.org/atomic v1.12.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
)

replace github.com/divine-ai-com/flightrec => ../..

replace github.com/divine-ai-com/flightrec/notifier/redisnotifier => ../../notifier/redisnotifier
