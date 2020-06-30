package chord

import "errors"

var (
	// ErrNotFound represents node not found error
	ErrNotFound = errors.New("NotFound")
	// ErrStabilizeNotCompleted represents stabilize process not completed error
	ErrStabilizeNotCompleted = errors.New("StabilizeNotCompleted")
	// ErrNodeUnavailable represents no node available error
	ErrNodeUnavailable = errors.New("NodeUnavailable")
	// ErrNoSuccessorAlive represents no successor available error
	ErrNoSuccessorAlive = errors.New("ErrNoSuccessorAlive")
)
