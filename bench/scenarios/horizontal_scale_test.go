package scenarios

import (
	"strings"
	"testing"
)

func TestHorizontalScaleComputeScalingVerdict(t *testing.T) {
	tests := []struct {
		name     string
		runs     []horizontalScaleDenseRun
		wantMode string
		wantPass bool
	}{
		{
			name:     "throughput improvement",
			runs:     []horizontalScaleDenseRun{{ReplicaCount: 1, Throughput: 100}, {ReplicaCount: 2, Throughput: 180}},
			wantMode: "improvement",
			wantPass: true,
		},
		{
			name:     "no improvement",
			runs:     []horizontalScaleDenseRun{{ReplicaCount: 1, Throughput: 150}, {ReplicaCount: 2, Throughput: 150}},
			wantMode: "no-improvement",
			wantPass: true,
		},
		{
			name:     "regression",
			runs:     []horizontalScaleDenseRun{{ReplicaCount: 1, Throughput: 180}, {ReplicaCount: 2, Throughput: 120}},
			wantMode: "regression",
			wantPass: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := horizontalScaleComputeScalingVerdict(tc.runs)
			if got.Mode != tc.wantMode {
				t.Fatalf("mode=%q want=%q", got.Mode, tc.wantMode)
			}
			if got.Pass != tc.wantPass {
				t.Fatalf("pass=%v want=%v", got.Pass, tc.wantPass)
			}
		})
	}
}

func TestHorizontalScalePerReplicaDistributionVerdict(t *testing.T) {
	tests := []struct {
		name         string
		counts       map[string]int
		expectedIDs  []string
		wantHardFail bool
		wantContains string
	}{
		{
			name:         "zero-work replica hard fail",
			counts:       map[string]int{"replica-1": 10, "replica-2": 0},
			expectedIDs:  []string{"replica-1", "replica-2"},
			wantHardFail: true,
		},
		{
			name:         "imbalance diagnostic only",
			counts:       map[string]int{"replica-1": 200, "replica-2": 100},
			expectedIDs:  []string{"replica-1", "replica-2"},
			wantHardFail: false,
			wantContains: "imbalance ratio",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := horizontalScalePerReplicaDistributionVerdict(tc.counts, tc.expectedIDs)
			if got.HardFail != tc.wantHardFail {
				t.Fatalf("hardFail=%v want=%v", got.HardFail, tc.wantHardFail)
			}
			if tc.wantContains != "" && !strings.Contains(got.Diagnostic, tc.wantContains) {
				t.Fatalf("diagnostic=%q want contains %q", got.Diagnostic, tc.wantContains)
			}
		})
	}
}
