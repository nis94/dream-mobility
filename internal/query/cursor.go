package query

import (
	"encoding/base64"
	"fmt"
	"strings"
	"time"
)

// Cursor is the pagination cursor for ListEvents. It carries the (event_ts,
// event_id) of the last row served on the previous page so the next page's
// row-wise SQL comparison can tie-break events that share a microsecond.
//
// A cursor without event_id would be lossy at microsecond resolution: two
// events for the same entity at the same event_ts would be skipped between
// pages under a strict `event_ts < cursor` predicate. Including event_id in
// both the cursor and the SQL ORDER BY closes that hole.
type Cursor struct {
	EventTS time.Time
	EventID string
}

// IsZero reports whether the cursor is unset (i.e. "start from the top").
func (c Cursor) IsZero() bool {
	return c.EventTS.IsZero() && c.EventID == ""
}

// Encode returns the opaque URL-safe base64 token for this cursor. The zero
// cursor encodes to the empty string so "no cursor" round-trips cleanly.
func (c Cursor) Encode() string {
	if c.IsZero() {
		return ""
	}
	raw := c.EventTS.UTC().Format(time.RFC3339Nano) + "|" + c.EventID
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

// DecodeCursor parses an opaque cursor token. An empty string decodes to the
// zero cursor with no error. Any non-empty token must base64-decode to
// "<rfc3339nano>|<event_id>" with both parts non-empty.
func DecodeCursor(s string) (Cursor, error) {
	if s == "" {
		return Cursor{}, nil
	}
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return Cursor{}, fmt.Errorf("invalid cursor encoding: %w", err)
	}
	parts := strings.SplitN(string(b), "|", 2)
	if len(parts) != 2 {
		return Cursor{}, fmt.Errorf("cursor missing separator")
	}
	if parts[0] == "" || parts[1] == "" {
		return Cursor{}, fmt.Errorf("cursor parts must be non-empty")
	}
	ts, err := time.Parse(time.RFC3339Nano, parts[0])
	if err != nil {
		return Cursor{}, fmt.Errorf("invalid cursor timestamp: %w", err)
	}
	return Cursor{EventTS: ts, EventID: parts[1]}, nil
}
