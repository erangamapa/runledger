package pricing

import "testing"

func TestCostMicros(t *testing.T) {
	tb := Default()
	cases := []struct {
		model   string
		in, out int
		want    int64
	}{
		{"claude-sonnet-4-5", 1000, 500, 10_500},
		{"claude-sonnet-4-5-20250929", 1000, 500, 10_500},
		{"gpt-4o-mini", 1_000_000, 0, 150_000},
		{"erangamapa/bet-brief-drafter-llama-3.1-8b-qlora", 9000, 9000, 0},
		{"some-new-model", 1000, 1000, 0},
	}
	for _, c := range cases {
		if got := tb.CostMicros(c.model, c.in, c.out); got != c.want {
			t.Errorf("%s: want %d got %d", c.model, c.want, got)
		}
	}
}

func TestLongestPrefixWins(t *testing.T) {
	tb, err := Parse([]byte("gpt-4o: {input: 2.5, output: 10}\ngpt-4o-mini: {input: 0.15, output: 0.6}\n"))
	if err != nil {
		t.Fatal(err)
	}
	p, ok := tb.Lookup("gpt-4o-mini-2024-07-18")
	if !ok || p.Input != 0.15 {
		t.Fatalf("mini snapshot priced as %+v", p)
	}
}

func TestRejectsNegative(t *testing.T) {
	if _, err := Parse([]byte("x: {input: -1, output: 0}\n")); err == nil {
		t.Fatal("negative price should fail")
	}
}
