package processor

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/segmentio/kafka-go"
)

// TestProcessMessage_DecodeFailureIncrementsCounter verifies that a message
// that cannot be decoded increments the decodeFailures counter and does NOT
// return an error (so the offset will be committed past the poison pill).
// The store is nil here — processMessage must short-circuit before touching
// it on decode failure.
func TestProcessMessage_DecodeFailureIncrementsCounter(t *testing.T) {
	p := &Processor{
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	cases := []struct {
		name string
		data []byte
	}{
		{"too short", []byte{0x00, 0x00}},
		{"bad magic byte", []byte{0x01, 0x00, 0x00, 0x00, 0x01, 0xff}},
		{"truncated avro", []byte{0x00, 0x00, 0x00, 0x00, 0x01, 0xff}},
		{"nil payload", nil},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := p.decodeFailures.Load()
			err := p.processMessage(context.Background(), kafka.Message{Value: tc.data})
			if err != nil {
				t.Fatalf("processMessage returned error %v; should swallow decode failures", err)
			}
			after := p.decodeFailures.Load()
			if after != before+1 {
				t.Errorf("decodeFailures = %d, want %d (before=%d)", after, before+1, before)
			}
		})
	}
}
