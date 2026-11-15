// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

// Package otelexport benchmarks otelflightrec.SpanProcessor against a real
// export path: a BatchSpanProcessor and the OTLP/HTTP exporter, which
// serializes spans to protobuf and sends them to a local HTTP server.
//
// It is a separate module so the exporter's dependencies (gRPC, protobuf)
// never reach otelflightrec's users. It is not published.
package otelexport
