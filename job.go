package worker

import "encoding/json"

// Job represents a job taken from the queue.
type Job struct {
	ID        int64
	TaskID    string
	QueueName string

	payload []byte
}

// UnmarshalPayload parses the JSON-encoded data and stores
// the result in the value pointed to by v.
// If v is nil or not a pointer, Unmarshal returns a
// json.InvalidUnmarshalError.
func (j *Job) UnmarshalPayload(v interface{}) error {
	return json.Unmarshal(j.payload, v)
}
