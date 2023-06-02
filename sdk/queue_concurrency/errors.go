package queue_concurrency

import "errors"

var (
	ErrorNotFound         = errors.New("entity not found") // entitify being connection/queue/delivery
	ErrorAlreadyConsuming = errors.New("must not call StartConsuming() multiple times")
	ErrorNotConsuming     = errors.New("must call StartConsuming() before adding consumers")
	ErrorConsumingStopped = errors.New("consuming stopped")
)
