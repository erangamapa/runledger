// Package pricing turns token counts into money.
//
// Cost is stored as integer micro dollars. Floats are fine for one call but
// rollups over a month of spans drift visibly in the cents column.
package pricing

import (
	_ "embed"
	"fmt"
	"math"
	"strings"

	"gopkg.in/yaml.v3"
)

//go:embed prices.yaml
var defaultPrices []byte

// Price is dollars per million tokens, the unit every provider publishes.
type Price struct {
	Input  float64 `yaml:"input"`
	Output float64 `yaml:"output"`
}

type Table struct {
	prices map[string]Price
}

func Default() *Table {
	t, err := Parse(defaultPrices)
	if err != nil {
		panic(fmt.Sprintf("embedded price table is broken: %v", err))
	}
	return t
}

func Parse(raw []byte) (*Table, error) {
	var prices map[string]Price
	if err := yaml.Unmarshal(raw, &prices); err != nil {
		return nil, err
	}
	norm := make(map[string]Price, len(prices))
	for k, p := range prices {
		if p.Input < 0 || p.Output < 0 {
			return nil, fmt.Errorf("negative price for %s", k)
		}
		norm[strings.ToLower(k)] = p
	}
	return &Table{prices: norm}, nil
}

// Lookup matches exact names first, then the longest prefix, so dated
// snapshots like claude-sonnet-4-5-20250929 price the same as the alias.
func (t *Table) Lookup(model string) (Price, bool) {
	m := strings.ToLower(model)
	if p, ok := t.prices[m]; ok {
		return p, true
	}
	best, bestLen := Price{}, 0
	for k, p := range t.prices {
		if strings.HasPrefix(m, k) && len(k) > bestLen {
			best, bestLen = p, len(k)
		}
	}
	return best, bestLen > 0
}

// CostMicros is zero for unknown models. Self hosted models are listed with
// a zero price on purpose, their cost is the gpu bill not the token count.
func (t *Table) CostMicros(model string, in, out int) int64 {
	p, ok := t.Lookup(model)
	if !ok {
		return 0
	}
	// a price per million tokens times tokens is already micro dollars
	return int64(math.Round(p.Input*float64(in) + p.Output*float64(out)))
}
