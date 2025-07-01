package tokens

import (
	"time"

	"math/rand"

	"github.com/oklog/ulid/v2"
)

func GenerateULID() string {
	entropy := rand.New(rand.NewSource(time.Now().UnixNano()))
	ms := ulid.Timestamp(time.Now())
	return ulid.MustNew(ms, entropy).String()
}
