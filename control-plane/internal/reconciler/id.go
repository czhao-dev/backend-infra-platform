package reconciler

import (
	"crypto/rand"
	"encoding/hex"
)

func newJobID() (string, error) {
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return "job_" + hex.EncodeToString(b), nil
}
