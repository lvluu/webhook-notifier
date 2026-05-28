// Package ingest accepts Events and fans them out to Deliveries.
//
// One Event is written together with its derived Delivery rows in the
// same transaction so no Delivery is ever orphaned from its Event.
//
// See docs/DESIGN.md.
package ingest
