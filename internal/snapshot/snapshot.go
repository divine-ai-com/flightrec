// Copyright 2026 The flightrec Authors
// SPDX-License-Identifier: Apache-2.0

// Package snapshot copies slog.Any values so they can be written after the
// objects they point to have changed: for retained scopes and the crash journal.
// A copy renders as slog's JSON and text handlers render the original.
package snapshot

import (
	"bytes"
	"encoding"
	"encoding/json"
	"fmt"
	"log/slog"
	"reflect"
)

// Copier copies values for one retained scope. The same pointer is
// often logged many times in one request, so pointer values are encoded once.
type Copier struct {
	Budget   int64 // stop copying once Added exceeds it; 0 means no limit
	Added    int64 // bytes the copies hold
	pointers map[any]slog.Value
}

// HasAny reports whether v holds a slog.KindAny value that is not a copy yet.
func HasAny(v slog.Value) bool {
	switch v.Kind() {
	case slog.KindAny:
		_, done := v.Any().(Value)
		return !done
	case slog.KindGroup:
		for _, a := range v.Group() {
			if HasAny(a.Value) {
				return true
			}
		}
	}
	return false
}

// Attr returns a with its slog.KindAny values, nested in groups too, copied.
func (sn *Copier) Attr(a slog.Attr) slog.Attr {
	switch a.Value.Kind() {
	case slog.KindAny:
		v := a.Value.Any()
		if _, done := v.(Value); done {
			return a
		}
		isPointer := v != nil && reflect.TypeOf(v).Kind() == reflect.Pointer
		if isPointer {
			if cached, ok := sn.pointers[v]; ok {
				a.Value = cached
				return a
			}
		}
		if sn.Budget > 0 && sn.Added > sn.Budget {
			// Copying more could not be retained anyway: stop paying for it.
			a.Value = slog.StringValue("!TRUNCATED: retained budget exceeded")
			return a
		}
		a.Value = Of(v)
		if s, ok := a.Value.Any().(Value); ok {
			sn.Added += s.size()
		} else {
			sn.Added += int64(len(a.Value.String()))
		}
		if isPointer {
			if sn.pointers == nil {
				sn.pointers = make(map[any]slog.Value)
			}
			sn.pointers[v] = a.Value
		}
	case slog.KindGroup:
		group := a.Value.Group()
		attrs := make([]slog.Attr, len(group))
		for i, ga := range group {
			attrs[i] = sn.Attr(ga)
		}
		a.Value = slog.GroupValue(attrs...)
	}
	return a
}

// maxValue bounds one copied slog.Any value, in each of its two
// renderings. Larger values are replaced by a short marker.
const maxValue = 64 << 10

// Truncated replaces a value too large to copy.
const Truncated = "!TRUNCATED: value larger than 64 KB"

// Of copies v as the two renderings handlers use: JSON, as
// slog.JSONHandler writes values (json.Marshal, honouring MarshalJSON), and
// text, as slog.TextHandler writes them (MarshalText if v has it, otherwise
// fmt's %+v, which honours String). Keeping the text rendering preserves
// redaction that a type does in String or MarshalText.
func Of(v any) slog.Value {
	raw := renderJSON(v)
	text := renderText(v)
	if len(raw) > maxValue {
		raw, _ = json.Marshal(Truncated)
	}
	if len(text) > maxValue {
		text = Truncated
	}
	return slog.AnyValue(Value{raw: raw, text: text})
}

// renderJSON renders v as slog.JSONHandler does: an error that is not a
// json.Marshaler as its message, anything else through encoding/json.
func renderJSON(v any) (raw []byte) {
	defer func() {
		if r := recover(); r != nil {
			raw, _ = json.Marshal(panicText(v, r))
		}
	}()
	if _, isMarshaler := v.(json.Marshaler); !isMarshaler {
		if err, ok := v.(error); ok {
			raw, _ = json.Marshal(err.Error())
			return raw
		}
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		raw, _ = json.Marshal("!ERROR:" + err.Error())
		return raw
	}
	return bytes.TrimRight(buf.Bytes(), "\n")
}

// renderText renders v as slog.TextHandler does: MarshalText if v has it,
// a byte slice as its string (TextHandler quotes it when needed), anything
// else with fmt's %+v (which uses Format, Error or String).
func renderText(v any) (text string) {
	defer func() {
		if r := recover(); r != nil {
			text = panicText(v, r)
		}
	}()
	if tm, ok := v.(encoding.TextMarshaler); ok {
		b, err := tm.MarshalText()
		if err != nil {
			return "!ERROR:" + err.Error()
		}
		return string(b)
	}
	if rv := reflect.ValueOf(v); rv.Kind() == reflect.Slice && rv.Type().Elem().Kind() == reflect.Uint8 {
		return string(rv.Bytes())
	}
	return fmt.Sprintf("%+v", v)
}

// panicText is what slog writes when rendering a value panics: <nil> for a
// nil pointer (a value-receiver method called through it), else the panic.
func panicText(v any, r any) string {
	if rv := reflect.ValueOf(v); rv.Kind() == reflect.Pointer && rv.IsNil() {
		return "<nil>"
	}
	return fmt.Sprintf("!PANIC: %v", r)
}

// Value is a copy of a slog.Any value taken when its scope was
// retained or journaled. slog.JSONHandler writes it as the original JSON;
// slog.TextHandler and fmt write the text the original value produced.
type Value struct {
	raw  []byte
	text string
}

// MarshalJSON implements json.Marshaler.
func (v Value) MarshalJSON() ([]byte, error) { return v.raw, nil }

// MarshalText implements encoding.TextMarshaler.
func (v Value) MarshalText() ([]byte, error) { return []byte(v.text), nil }

// String implements fmt.Stringer.
func (v Value) String() string { return v.text }

// size is the memory the copy holds.
func (v Value) size() int64 { return int64(len(v.raw) + len(v.text)) }

// Make builds a Value from its two renderings, as the journal stores them.
func Make(raw []byte, text string) Value { return Value{raw: raw, text: text} }

// Parts returns the JSON and text renderings of v.
func Parts(v Value) (raw []byte, text string) { return v.raw, v.text }
