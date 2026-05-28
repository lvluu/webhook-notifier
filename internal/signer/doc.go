// Package signer produces HMAC-SHA256 signatures for outbound Webhook POSTs.
//
// Signature is computed over `timestamp.body`; the receiver verifies
// using the shared per-Webhook secret.
package signer
