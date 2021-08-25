package easycb

import "time"

func withRetry(num int, delay time.Duration, fn func() (bool, error)) error {
	var lastErr error
	for i := 0; i < num; i++ {
		shouldRetry, err := fn()
		lastErr = err
		if shouldRetry {
			time.Sleep(delay)
			continue
		} else {
			break
		}
	}

	return lastErr
}
