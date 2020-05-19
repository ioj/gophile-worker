package worker

import (
	"fmt"
	"math/rand"
)

// Opts contains parameters for worker.New().
type Opts struct {
	// Worker ID. Default "worker-<randomstring>".
	ID string

	// The database schema in which Graphile Worker is located.
	// Default: "graphile_worker".
	Schema string
}

func optsWithDefaults(orig *Opts) *Opts {
	opts := &Opts{}
	if orig != nil {
		*opts = *orig
	}

	if opts.ID == "" {
		letters := "01234567890abcdef"
		b := make([]byte, 18)
		for i := range b {
			b[i] = letters[rand.Intn(len(letters))]
		}
		opts.ID = fmt.Sprintf("worker-%v", string(b))
	}

	if opts.Schema == "" {
		opts.Schema = "graphile_worker"
	}

	return opts
}
