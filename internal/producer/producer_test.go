package producer

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// newTestLogger returns a slog logger that discards output. Use in tests that
// don't need to inspect log lines.
func newTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestRegisterSchema_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		if r.URL.Path != "/subjects/movement.events-value/versions" {
			t.Errorf("path = %s, want /subjects/movement.events-value/versions", r.URL.Path)
		}
		if ct := r.Header.Get("Content-Type"); ct != "application/vnd.schemaregistry.v1+json" {
			t.Errorf("Content-Type = %q, want application/vnd.schemaregistry.v1+json", ct)
		}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]int{"id": 42})
	}))
	defer srv.Close()

	id, err := registerSchema(context.Background(), srv.URL, "movement.events-value", []byte(`{"type":"string"}`), newTestLogger())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if id != 42 {
		t.Errorf("id = %d, want 42", id)
	}
}

func TestRegisterSchema_RetryOn5xxThenSuccess(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&calls, 1)
		// Verify each retry builds a fresh request with full body.
		body, _ := io.ReadAll(r.Body)
		if !strings.Contains(string(body), `"schemaType"`) {
			t.Errorf("attempt %d: body missing schemaType: %s", n, body)
		}
		if n < 3 {
			http.Error(w, "upstream down", http.StatusBadGateway)
			return
		}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]int{"id": 7})
	}))
	defer srv.Close()

	// Patch the package-level client to skip sleep in the backoff. We can't
	// directly; the default 5s timeout per request is fine, and the test's
	// linear backoff (1s+2s) is tolerable here.
	id, err := registerSchema(context.Background(), srv.URL, "test-value", []byte(`{}`), newTestLogger())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if id != 7 {
		t.Errorf("id = %d, want 7", id)
	}
	if got := atomic.LoadInt32(&calls); got != 3 {
		t.Errorf("calls = %d, want 3", got)
	}
}

func TestRegisterSchema_FatalOn4xxOtherThanRetriable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error_code":42201,"message":"Schema is invalid"}`, http.StatusUnprocessableEntity)
	}))
	defer srv.Close()

	_, err := registerSchema(context.Background(), srv.URL, "test-value", []byte(`{}`), newTestLogger())
	if err == nil {
		t.Fatal("expected error on 422")
	}
	if !strings.Contains(err.Error(), "422") {
		t.Errorf("error should mention 422: %v", err)
	}
}

func TestRegisterSchema_CtxCancel(t *testing.T) {
	// Server always returns 500 so the retry loop runs.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	// Cancel after a short delay so the first backoff trips ctx.Done.
	go func() {
		time.Sleep(150 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	_, err := registerSchema(ctx, srv.URL, "test-value", []byte(`{}`), newTestLogger())
	if err == nil {
		t.Fatal("expected error on ctx cancel")
	}
	// Should exit well before the full 5-attempt budget (~10s).
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("took too long to exit on ctx cancel: %v", elapsed)
	}
	if !isContextError(err) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
}

func isContextError(err error) bool {
	for err != nil {
		if err == context.Canceled || err == context.DeadlineExceeded {
			return true
		}
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}

func TestEncodeWireFormat(t *testing.T) {
	p := &Producer{schemaID: 0x01020304}
	payload := []byte{0xAA, 0xBB, 0xCC}

	got := p.encodeWireFormat(payload)

	if len(got) != 5+len(payload) {
		t.Fatalf("len = %d, want %d", len(got), 5+len(payload))
	}
	if got[0] != 0x00 {
		t.Errorf("magic byte = 0x%02X, want 0x00", got[0])
	}
	if id := binary.BigEndian.Uint32(got[1:5]); id != 0x01020304 {
		t.Errorf("schema id = 0x%08X, want 0x01020304", id)
	}
	for i, b := range payload {
		if got[5+i] != b {
			t.Errorf("payload[%d] = 0x%02X, want 0x%02X", i, got[5+i], b)
		}
	}
}

func TestEncodeWireFormat_Empty(t *testing.T) {
	p := &Producer{schemaID: 0}
	got := p.encodeWireFormat(nil)
	if len(got) != 5 {
		t.Fatalf("len = %d, want 5", len(got))
	}
	if got[0] != 0x00 {
		t.Errorf("magic byte = 0x%02X, want 0x00", got[0])
	}
}
