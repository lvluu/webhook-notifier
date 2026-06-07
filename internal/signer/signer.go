package signer

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"time"
)

// Sign returns the hex-encoded HMAC-SHA256 signature for a timestamped body.
// Pre-allocates the payload as []byte to avoid string conversion round-trips.
func Sign(secret, body []byte, ts time.Time) string {
	payload := make([]byte, 0, 12+len(body))
	payload = strconv.AppendInt(payload, ts.Unix(), 10)
	payload = append(payload, '.')
	payload = append(payload, body...)
	mac := hmac.New(sha256.New, secret)
	mac.Write(payload)
	return hex.EncodeToString(mac.Sum(nil))
}
