package monitor

import "context"

// Sampler collects resource usage between Start and Stop.
type Sampler interface {
	Start(ctx context.Context)
	Stop() Usage
}
