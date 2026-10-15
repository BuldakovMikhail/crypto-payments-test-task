package payments

import (
	"encoding/json"
	"testing"
)

func TestAmounts(t *testing.T) {
	for _, tc := range []struct{ in, out string }{{"0", "0.000000"}, {"0.000001", "0.000001"}, {"100.123456", "100.123456"}, {"1000000000", "1000000000.000000"}} {
		a, err := ParseAmount(tc.in)
		if err != nil || a.String() != tc.out {
			t.Fatalf("%s: %v %s", tc.in, err, a)
		}
		b, err := json.Marshal(a)
		if err != nil || string(b) != `"`+tc.out+`"` {
			t.Fatalf("JSON %s: %s %v", tc.in, b, err)
		}
	}
	for _, in := range []string{"", "-1", "+1", "1e6", " 1", "1.", ".1", "1.0000001", "1000000000.000001", "9223372036854775807", "1..2"} {
		if _, err := ParseAmount(in); err == nil {
			t.Fatalf("accepted %q", in)
		}
	}
	var a Amount
	for _, in := range []string{`1.2`, `null`, `true`, `{}`} {
		if err := json.Unmarshal([]byte(in), &a); err == nil {
			t.Fatalf("accepted JSON %s", in)
		}
	}
}
func TestFeesAndLargeLedgerTotals(t *testing.T) {
	for _, tc := range []struct {
		gross       Amount
		bps         int
		fixed, want Amount
	}{
		{100_000_000, 150, 100_000, 1_600_000},
		{1, 150, 0, 0},
		{1, 10000, 100_000, 1},
		{MaxAmount, 9999, 0, 999_900_000_000_000},
	} {
		if got := Fee(tc.gross, tc.bps, tc.fixed); got != tc.want {
			t.Fatalf("fee: got %d want %d", got, tc.want)
		}
	}
	if got := formatUnits("922337203685477580800"); got != "922337203685477.580800" {
		t.Fatal(got)
	}
	if got := formatUnits("-1"); got != "-0.000001" {
		t.Fatal(got)
	}
}
