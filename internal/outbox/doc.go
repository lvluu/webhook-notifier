// Package outbox handles Delivery claim and persistence against Postgres.
//
// Claim semantics use `SELECT ... FOR UPDATE SKIP LOCKED` so multiple
// workers across multiple instances cannot double-deliver (ADR-0002).
package outbox
