package avro

// Regenerate movement_event.go from the Avro schema:
//
//go:generate go run github.com/hamba/avro/v2/cmd/avrogen -pkg avro -encoders -o movement_event.go ../../schemas/movement_event.avsc
