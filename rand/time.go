package rand

import (
	"math/rand"
	"time"
)

func GenerateDuration(lowerBound, upperBound time.Duration) time.Duration {
	if !isValidDuration(lowerBound, upperBound) {
		return 0
	}

	return time.Duration(rand.Int63n(int64(upperBound-lowerBound))) + lowerBound
}

func isValidDuration(lowerBound, upperBound time.Duration) bool {
	return lowerBound >= 0 && upperBound > lowerBound
}
