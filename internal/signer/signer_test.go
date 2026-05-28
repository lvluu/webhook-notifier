package signer

import (
	"testing"
	"time"
)

func TestSignerSign(t *testing.T) {
	tests := []struct {
		name   string
		secret []byte
		body   []byte
		ts     time.Time
		want   string
	}{
		{
			name:   "known vector",
			secret: []byte("test-secret"),
			body:   []byte(`{"event":"test"}`),
			ts:     time.Unix(1700000000, 0),
			want:   "ebc2eca832d45d54cec34edafc6db219b58d38eb9fdac1cac8f5b892ff4fd192",
		},
		{
			name:   "different secret",
			secret: []byte("other-secret"),
			body:   []byte(`{"event":"test"}`),
			ts:     time.Unix(1700000000, 0),
			want:   "6e4fd927166e58a95a20541b6b767b0387465acfb74e05f88b4edad4720ca735",
		},
		{
			name:   "different body",
			secret: []byte("test-secret"),
			body:   []byte(`{"event":"other"}`),
			ts:     time.Unix(1700000000, 0),
			want:   "b5e31187f2262d038f7c2b17ee0ddda8e89ec8e5d4f184909745a0f4af776f61",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Sign(tt.secret, tt.body, tt.ts)
			if got != tt.want {
				t.Errorf("Sign() = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestSignerSign_DifferentTimestamps(t *testing.T) {
	secret := []byte("test-secret")
	body := []byte(`{"event":"test"}`)

	sig1 := Sign(secret, body, time.Unix(1700000000, 0))
	sig2 := Sign(secret, body, time.Unix(1700000001, 0))

	if sig1 == sig2 {
		t.Error("different timestamps should produce different signatures")
	}
}
