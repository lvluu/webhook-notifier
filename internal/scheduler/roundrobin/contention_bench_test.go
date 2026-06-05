package roundrobin

import (
	"context"
	"fmt"
	"testing"

	"github.com/lvluu/webhook-notifier/internal/scheduler"
)

// BenchmarkPickContention measures the throughput ceiling of the single global
// scheduler mutex under maximal contention: every parallel goroutine does
// nothing but Pick. This quantifies finding B-4 / issue #78 — whether the
// global lock is a real bottleneck. The realistic worker loop hits the mutex
// roughly once per ClaimNextN batch (default 64 deliveries), each delivery
// costing a network round-trip, so the demanded pick rate is orders of
// magnitude below this ceiling. Run:
//
//	go test ./internal/scheduler/roundrobin/ -bench=Contention -run=^$ -cpu=1,8,64
func BenchmarkPickContention(b *testing.B) {
	for _, customers := range []int{1, 64, 4096} {
		b.Run(fmt.Sprintf("customers=%d", customers), func(b *testing.B) {
			s := New()
			for i := 0; i < customers; i++ {
				s.Notify(scheduler.CustomerID(fmt.Sprintf("cus_%d", i)))
			}
			ctx := context.Background()

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					_, _, _, _ = s.Pick(ctx)
				}
			})
		})
	}
}

// BenchmarkSchedulerMixedContention models the real Notify/Pick/Remove mix a
// worker churns: a Pick followed by an occasional membership mutation, all on
// the shared mutex.
func BenchmarkSchedulerMixedContention(b *testing.B) {
	s := New()
	const seeded = 4096
	for i := 0; i < seeded; i++ {
		s.Notify(scheduler.CustomerID(fmt.Sprintf("cus_%d", i)))
	}
	ctx := context.Background()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			cid := scheduler.CustomerID(fmt.Sprintf("cus_%d", i%seeded))
			s.Pick(ctx)
			if i%64 == 0 {
				// One membership mutation per batch-sized run of picks,
				// matching the worker loop's Notify/Remove cadence.
				s.Notify(cid)
			}
			i++
		}
	})
}
