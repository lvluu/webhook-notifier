package scenarios

import "testing"

func TestComparePartnerProtectionLatencies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		control    []healthyCustomerLatency
		dead       []healthyCustomerLatency
		wantRatio  float64
		wantWithin bool
		wantErr    bool
	}{
		{
			name:       "within limit",
			control:    []healthyCustomerLatency{{CustomerID: "small-1", ControlP95: 100}},
			dead:       []healthyCustomerLatency{{CustomerID: "small-1", ControlP95: 109}},
			wantRatio:  1.09,
			wantWithin: true,
		},
		{
			name:       "over limit",
			control:    []healthyCustomerLatency{{CustomerID: "small-1", ControlP95: 100}},
			dead:       []healthyCustomerLatency{{CustomerID: "small-1", ControlP95: 111}},
			wantRatio:  1.11,
			wantWithin: false,
		},
		{
			name:    "missing dead customer fails closed",
			control: []healthyCustomerLatency{{CustomerID: "small-1", ControlP95: 100}},
			wantErr: true,
		},
		{
			name:    "mismatched customer fails closed",
			control: []healthyCustomerLatency{{CustomerID: "small-1", ControlP95: 100}},
			dead:    []healthyCustomerLatency{{CustomerID: "small-2", ControlP95: 100}},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, _, ratio, within, err := comparePartnerProtectionLatencies(tt.control, tt.dead)
			if (err != nil) != tt.wantErr {
				t.Fatalf("error = %v, wantErr %t", err, tt.wantErr)
			}
			if err != nil {
				return
			}
			if ratio != tt.wantRatio {
				t.Fatalf("ratio = %v, want %v", ratio, tt.wantRatio)
			}
			if within != tt.wantWithin {
				t.Fatalf("within = %t, want %t", within, tt.wantWithin)
			}
		})
	}
}
