package chord

import "errors"

var (
	ErrNotFound              = errors.New("NotFound")
	ErrNotCompletedStabilize = errors.New("NotCompleteStabilize")
	ErrDialFailed            = errors.New("DialFailed")
)
