package tokens

import "testing"

func TestGenerateULID(t *testing.T) {
	t.Run("check if the length of the generated ULID is 26", func(t *testing.T) {
		ulid := GenerateULID()
		if len(ulid) != 26 {
			t.Errorf("expected: 26, got: %d", len(ulid))
		}
	})
}
