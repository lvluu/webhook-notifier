package signer

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"
)

// Sign returns the hex-encoded HMAC-SHA256 signature for a timestamped body.
func Sign(secret, body []byte, ts time.Time) string {
	payload := fmt.Sprintf("%d.%s", ts.Unix(), string(body))
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(payload))
	return hex.EncodeToString(mac.Sum(nil))
}
