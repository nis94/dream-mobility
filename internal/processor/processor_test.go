package processor

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/segmentio/kafka-go"
)

// TestProcessMessage_DecodeFailureSwallows verifies that a message that
// cannot be decoded does NOT return an error (so the offset is committed
// past the poison pill). The store and metrics recorder are both nil here
// — processMessage must short-circuit before touching either on decode
// failure. The corresponding decode-failure metric increment is covered
// by internal/kafkametrics unit tests.
func TestProcessMessage_DecodeFailureSwallows(t *testing.T) {
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
			err := p.processMessage(context.Background(), kafka.Message{Value: tc.data})
			if err != nil {
				t.Fatalf("processMessage returned error %v; should swallow decode failures", err)
			}
		})
	}
}
