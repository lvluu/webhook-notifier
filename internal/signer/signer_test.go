package signer

import (
	"strings"
	"testing"
	"time"
)

func TestSignerSign_EmptyBody(t *testing.T) {
	secret := []byte("test-secret")
	body := []byte{}
	ts := time.Unix(1700000000, 0)

	got := Sign(secret, body, ts)
	if got == "" {
		t.Error("Sign() with empty body should produce a signature")
	}
	if strings.Contains(got, "0.") {
		t.Error("Sign() should not contain raw timestamp in output")
	}
}

func TestSignerSign_EmptySecret(t *testing.T) {
	secret := []byte{}
	body := []byte(`{"event":"test"}`)
	ts := time.Unix(1700000000, 0)

	got := Sign(secret, body, ts)
	if got == "" {
		t.Error("Sign() with empty secret should produce a signature")
	}
}

func TestSignerSign_LargeBody(t *testing.T) {
	secret := []byte("test-secret")
	body := []byte(strings.Repeat("A", 10000))
	ts := time.Unix(1700000000, 0)

	got := Sign(secret, body, ts)
	if got == "" {
		t.Error("Sign() with large body should produce a signature")
	}
	if len(got) != 64 {
		t.Errorf("Sign() should produce 64-char hex, got %d chars", len(got))
	}
}

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

func BenchmarkSignerSign(b *testing.B) {
	secret := []byte("test-secret")
	body := []byte(`{"event":"test"}`)
	ts := time.Unix(1700000000, 0)

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		Sign(secret, body, ts)
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
