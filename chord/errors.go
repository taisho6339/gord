package chord

import "errors"

var (
	ErrNotFound              = errors.New("NotFound")
	ErrStabilizeNotCompleted = errors.New("StabilizeNotCompleted")
	ErrNodeUnavailable       = errors.New("NodeUnavailable")
	ErrNoSuccessorAlive      = errors.New("ErrNoSuccessorAlive")
)
