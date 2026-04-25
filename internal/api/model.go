package api

// EventRequest is the JSON shape arriving from external clients (smoke tests,
// the synthetic generator, future external producers). The aviation schema is
// flat — there's no per-field nesting like the old entity:/position: wrappers,
// because every field belongs to a single observation about one aircraft.
type EventRequest struct {
	EventID         string   `json:"event_id"`
	Icao24          string   `json:"icao24"`
	Callsign        *string  `json:"callsign,omitempty"`
	OriginCountry   string   `json:"origin_country"`
	ObservedAt      string   `json:"observed_at"`
	PositionSource  string   `json:"position_source"`
	Lat             float64  `json:"lat"`
	Lon             float64  `json:"lon"`
	BaroAltitudeM   *float64 `json:"baro_altitude_m,omitempty"`
	GeoAltitudeM    *float64 `json:"geo_altitude_m,omitempty"`
	VelocityMs      *float64 `json:"velocity_ms,omitempty"`
	TrueTrackDeg    *float64 `json:"true_track_deg,omitempty"`
	VerticalRateMs  *float64 `json:"vertical_rate_ms,omitempty"`
	OnGround        bool     `json:"on_ground"`
	Squawk          *string  `json:"squawk,omitempty"`
	Spi             bool     `json:"spi,omitempty"`
	Category        *int     `json:"category,omitempty"`
}

// BatchRequest wraps multiple events for the batch ingestion path.
type BatchRequest struct {
	// Events is the list of events to ingest. An empty slice is a well-formed
	// no-op and returns 202 with zero accepted/rejected counts.
	Events []EventRequest `json:"events"`
}

// IngestResponse is returned by POST /events.
type IngestResponse struct {
	// Accepted is the number of events successfully produced to Kafka.
	Accepted int `json:"accepted"`
	// Rejected is the number of events dropped due to validation failure or
	// Kafka produce error. The per-event reason is in Errors.
	Rejected int `json:"rejected"`
	// Errors enumerates each rejection by its index in the input.
	Errors []EventError `json:"errors,omitempty"`
}

// EventError describes a single validation or produce failure within a batch.
type EventError struct {
	// Index is the position of the failing event in the input (0-based).
	Index int `json:"index"`
	// EventID is the event_id of the failing event, when present in the input.
	EventID string `json:"event_id,omitempty"`
	// Error is a human-readable description of the failure.
	Error string `json:"error"`
}
