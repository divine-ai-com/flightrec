// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

package snapshot

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestSnapshotBudget(t *testing.T) {
	big := map[string]string{}
	for i := range 4000 {
		big[strconv.Itoa(i)] = strings.Repeat("x", 30)
	}
	if v := Of(big).Any().(Value); string(v.raw) != `"`+Truncated+`"` || v.text != Truncated {
		t.Errorf("value over %d bytes not truncated: %d/%d bytes", maxValue, len(v.raw), len(v.text))
	}
	sn := Copier{Budget: 100}
	sn.Attr(slog.Any("a", map[string]int{"k": 1}))
	sn.Added = 101
	if got := sn.Attr(slog.Any("b", map[string]int{"k": 2})); got.Value.String() != "!TRUNCATED: retained budget exceeded" {
		t.Errorf("copy past the budget: %v", got.Value)
	}
}

// redactedErr redacts itself in MarshalText, which slog.TextHandler prefers.
type redactedErr struct{}

func (redactedErr) Error() string                { return "secret-err" }
func (redactedErr) MarshalText() ([]byte, error) { return []byte("redacted-err"), nil }

// TestSnapshotRendersLikeSlog compares the copy's renderings with what
// slog.TextHandler and slog.JSONHandler write for the original value.
func TestSnapshotRendersLikeSlog(t *testing.T) {
	var nilTime *time.Time
	for name, v := range map[string]any{
		"nil pointer with value MarshalText": nilTime,
		"error redacting in MarshalText":     redactedErr{},
		"plain error":                        errors.New("boom"),
		"map":                                map[string]int{"k": 1},
		"nil":                                nil,
	} {
		t.Run(name, func(t *testing.T) {
			render := func(h func(*bytes.Buffer) slog.Handler, val any) string {
				var buf bytes.Buffer
				r := slog.NewRecord(time.Time{}, slog.LevelInfo, "m", 0)
				r.AddAttrs(slog.Any("k", val))
				if err := h(&buf).Handle(context.Background(), r); err != nil {
					t.Fatal(err)
				}
				return buf.String()
			}
			text := func(b *bytes.Buffer) slog.Handler { return slog.NewTextHandler(b, nil) }
			js := func(b *bytes.Buffer) slog.Handler { return slog.NewJSONHandler(b, nil) }
			snap := Of(v).Any()
			if got, want := render(text, snap), render(text, v); got != want {
				t.Errorf("text: got %q, want %q", got, want)
			}
			if got, want := render(js, snap), render(js, v); got != want {
				t.Errorf("json: got %q, want %q", got, want)
			}
		})
	}
}

func TestSnapshotByteSlice(t *testing.T) {
	// slog.TextHandler always quotes a byte slice; the copy's text is the
	// same string, which TextHandler quotes when it needs to.
	if s := Of([]byte("a b")).Any().(Value); s.text != "a b" {
		t.Errorf("text %q", s.text)
	}
}

// TestValueShape pins what handlers can see of a Value through reflection:
// its exported methods and that it has no exported fields.
func TestValueShape(t *testing.T) {
	typ := reflect.TypeOf(Value{})
	var methods []string
	for i := range typ.NumMethod() {
		methods = append(methods, typ.Method(i).Name)
	}
	if got := strings.Join(methods, ","); got != "MarshalJSON,MarshalText,String" {
		t.Errorf("exported methods %s", got)
	}
	for i := range typ.NumField() {
		if typ.Field(i).IsExported() {
			t.Errorf("exported field %s", typ.Field(i).Name)
		}
	}
}
