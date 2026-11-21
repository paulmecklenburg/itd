package infinitime

import (
	"sync"
	"testing"
	"time"
)

func TestNormalizeMAC(t *testing.T) {
	tests := []struct {
		in      string
		want    string
		wantErr bool
	}{
		{in: "AA:BB:CC:DD:EE:FF", want: "AA:BB:CC:DD:EE:FF"},
		{in: "aa:bb:cc:dd:ee:ff", want: "AA:BB:CC:DD:EE:FF"},
		{in: "  11-22-33-aa-bb-cc ", want: "11:22:33:AA:BB:CC"},
		{in: "1122.3344.5566", want: "11:22:33:44:55:66"},
		{in: "", wantErr: true},
		{in: "not-a-mac", wantErr: true},
		{in: "AA:BB:CC:DD:EE", wantErr: true},
		{in: "AA:BB:CC:DD:EE:FF:00:11", wantErr: true}, // 8-byte EUI-64
	}

	for _, tc := range tests {
		got, err := NormalizeMAC(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Fatalf("NormalizeMAC(%q) = %q, want error", tc.in, got)
			}
			continue
		}
		if err != nil {
			t.Fatalf("NormalizeMAC(%q) unexpected error: %v", tc.in, err)
		}
		if got != tc.want {
			t.Fatalf("NormalizeMAC(%q) = %q, want %q", tc.in, got, tc.want)
		}
		if _, err := addressFromMAC(tc.in); err != nil {
			t.Fatalf("addressFromMAC(%q) unexpected error: %v", tc.in, err)
		}
	}
}

func TestDeviceIsAllowed(t *testing.T) {
	const (
		watch1 = "AA:BB:CC:DD:EE:01"
		watch2 = "AA:BB:CC:DD:EE:02"
		watch3 = "AA:BB:CC:DD:EE:03"
	)

	dynamicAllowed := watch2
	d := &Device{
		opts: Options{
			Allowlist: []string{"aa:bb:cc:dd:ee:01", "aa-bb-cc-dd-ee-02"},
			Blocklist: []string{watch3},
			AllowFunc: func(mac string) bool {
				return mac == dynamicAllowed
			},
		},
	}

	if d.isAllowed(watch1) {
		t.Fatalf("expected watch1 to be rejected when AllowFunc rejects it")
	}
	if !d.isAllowed(watch2) {
		t.Fatalf("expected watch2 to be allowed by both Allowlist and AllowFunc")
	}
	if d.isAllowed(watch3) {
		t.Fatalf("expected blocklisted watch3 to be rejected")
	}

	dynamicAllowed = watch3
	if d.isAllowed(watch3) {
		t.Fatalf("Blocklist must take precedence over AllowFunc")
	}
}

func TestDeviceDFUPinAndShouldAcceptCandidate(t *testing.T) {
	const (
		watch1 = "AA:BB:CC:DD:EE:01"
		watch2 = "AA:BB:CC:DD:EE:02"
		watch3 = "AA:BB:CC:DD:EE:03"
	)

	d := &Device{
		opts: Options{
			Allowlist: []string{watch1, watch2},
			Blocklist: []string{watch3},
		},
	}

	// DFU pin sets and expires properly.
	d.pinDFU("aa-bb-cc-dd-ee-01", 5*time.Second)
	if got := d.activeDFUPin(); got != watch1 {
		t.Fatalf("activeDFUPin() = %q, want %q", got, watch1)
	}
	d.pinDFU(watch1, -1*time.Second)
	if got := d.activeDFUPin(); got != "" {
		t.Fatalf("expired activeDFUPin() = %q, want empty", got)
	}
	d.pinDFU(watch1, 5*time.Second)
	d.clearDFUPin()
	if got := d.activeDFUPin(); got != "" {
		t.Fatalf("cleared activeDFUPin() = %q, want empty", got)
	}

	base := time.Unix(1_700_000_000, 0)

	// Explicit targetMAC bypasses Allowlist (watch2 vs a non-allowlisted MAC) while still respecting Blocklist.
	extraWatch := "11:22:33:44:55:66"
	if !d.shouldAcceptCandidate(extraWatch, false, extraWatch, "", watch1, base, true, base.Add(1*time.Second)) {
		t.Fatalf("explicit targetMAC should accept non-blocklisted target")
	}
	if d.shouldAcceptCandidate(watch1, true, extraWatch, "", "", time.Time{}, false, base) {
		t.Fatalf("explicit targetMAC should reject non-matching watch1")
	}
	if d.shouldAcceptCandidate(watch3, true, watch3, "", "", time.Time{}, false, base) {
		t.Fatalf("explicit targetMAC must reject blocklisted device")
	}

	// Active DFU pin keeps device locked to watch1 and rejects watch2.
	if !d.shouldAcceptCandidate(watch1, true, "", watch1, watch1, base, true, base.Add(1*time.Second)) {
		t.Fatalf("active DFU pin should accept watch1 even inside grace period")
	}
	if d.shouldAcceptCandidate(watch2, true, "", watch1, watch1, base, true, base.Add(1*time.Second)) {
		t.Fatalf("active DFU pin should reject watch2")
	}

	// Non-InfiniTime advertisement is rejected in auto mode.
	if d.shouldAcceptCandidate(watch2, false, "", "", "", time.Time{}, false, base) {
		t.Fatalf("should reject non-InfiniTime candidate when targetMAC is empty")
	}

	// 8s grace window rejects prevMAC early while immediately accepting watch2.
	if d.shouldAcceptCandidate(watch1, true, "", "", watch1, base, true, base.Add(3*time.Second)) {
		t.Fatalf("expected watch1 to be suppressed during 8s prevMAC grace period")
	}
	if !d.shouldAcceptCandidate(watch2, true, "", "", watch1, base, true, base.Add(3*time.Second)) {
		t.Fatalf("expected watch2 to be accepted immediately during watch1 grace period")
	}
	if !d.shouldAcceptCandidate(watch1, true, "", "", watch1, base, true, base.Add(prevMACGracePeriod)) {
		t.Fatalf("expected watch1 to be accepted once 8s grace period elapses")
	}
}

func TestDeviceAddressRaceFree(t *testing.T) {
	d := &Device{mac: "AA:BB:CC:DD:EE:01"}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			d.connStateMtx.Lock()
			if i%2 == 0 {
				d.mac = "AA:BB:CC:DD:EE:01"
			} else {
				d.mac = "AA:BB:CC:DD:EE:02"
			}
			d.connStateMtx.Unlock()
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			_ = d.Address()
			_ = d.IsConnected()
		}
	}()
	wg.Wait()
}
