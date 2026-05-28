// Package registry caches the Customer → []Webhook lookup.
//
// Read-only from the worker's perspective; backed by Postgres with an
// in-memory LRU + TTL front cache.
package registry
