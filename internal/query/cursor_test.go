package query

import (
	"encoding/base64"
	"testing"
	"time"
)

func TestCursor_EncodeDecodeRoundtrip(t *testing.T) {
	ts := time.Date(2025, 1, 1, 10, 0, 0, 123456789, time.UTC)
	in := Cursor{EventTS: ts, EventID: "00000000-0000-0000-0000-000000000001"}

	encoded := in.Encode()
	if encoded == "" {
		t.Fatal("encoded non-zero cursor should not be empty")
	}

	out, err := DecodeCursor(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !out.EventTS.Equal(in.EventTS) {
		t.Errorf("EventTS = %v, want %v", out.EventTS, in.EventTS)
	}
	if out.EventID != in.EventID {
		t.Errorf("EventID = %q, want %q", out.EventID, in.EventID)
	}
}

func TestCursor_ZeroRoundtrip(t *testing.T) {
	c := Cursor{}
	if !c.IsZero() {
		t.Fatal("empty cursor should be IsZero")
	}
	if enc := c.Encode(); enc != "" {
		t.Errorf("zero cursor Encode = %q, want empty", enc)
	}
	out, err := DecodeCursor("")
	if err != nil {
		t.Fatalf("decode empty: %v", err)
	}
	if !out.IsZero() {
		t.Error("decoded empty cursor should be IsZero")
	}
}

func TestDecodeCursor_BadInput(t *testing.T) {
	cases := []struct {
		name  string
		input string
	}{
		{"invalid base64", "!!!not-base64!!!"},
		{"missing separator", base64.RawURLEncoding.EncodeToString([]byte("no-pipe-here"))},
		{"empty timestamp", base64.RawURLEncoding.EncodeToString([]byte("|some-uuid"))},
		{"empty event id", base64.RawURLEncoding.EncodeToString([]byte("2025-01-01T10:00:00Z|"))},
		{"bad timestamp format", base64.RawURLEncoding.EncodeToString([]byte("not-a-date|some-uuid"))},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := DecodeCursor(tc.input); err == nil {
				t.Errorf("expected error for %q, got nil", tc.input)
			}
		})
	}
}

func TestCursor_OpacityIsUrlSafe(t *testing.T) {
	// Confirms the encoded cursor is safe to drop straight into a URL
	// query param without any additional escaping. RawURLEncoding uses
	// only [A-Za-z0-9_-], no padding.
	c := Cursor{
		EventTS: time.Date(2025, 6, 15, 12, 34, 56, 789000000, time.UTC),
		EventID: "abcd1234-5678-90ab-cdef-000000000042",
	}
	enc := c.Encode()
	for _, r := range enc {
		ok := (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') ||
			(r >= '0' && r <= '9') || r == '-' || r == '_'
		if !ok {
			t.Errorf("cursor contains URL-unsafe rune %q in %q", r, enc)
		}
	}
}
