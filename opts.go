package worker

import (
	"fmt"
	"math/rand"
	"time"
)

// Opts contains parameters for worker.New().
type Opts struct {
	// Worker ID. Default "worker-<randomstring>".
	ID string

	// The database schema in which Graphile Worker is located.
	// Default: "graphile_worker".
	Schema string

	// How long to wait between polling for jobs.
	//
	// Note: this does NOT need to be short, because we use LISTEN/NOTIFY to be
	// notified when new jobs are added - this is just used for jobs scheduled in
	// the future, retried jobs, and in the case where LISTEN/NOTIFY fails for
	// whatever reason. Default: 2 * time.Second
	PollInterval time.Duration
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

	if opts.PollInterval == 0 {
		opts.PollInterval = 2 * time.Second
	}

	return opts
}
