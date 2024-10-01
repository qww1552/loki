package drain

import (
	"strings"
	"time"
	"unique"

	"github.com/prometheus/common/model"

	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/pattern/iter"
)

type LogCluster struct {
	id         int
	Size       int
	Tokens     []unique.Handle[string]
	TokenState interface{}
	Stringer   func([]unique.Handle[string], interface{}) string

	Chunks Chunks
}

func (c *LogCluster) String() string {
	if c.Stringer != nil {
		return c.Stringer(c.Tokens, c.TokenState)
	}

	tokens := make([]string, 0, len(c.Tokens))
	for _, t := range c.Tokens {
		tokens = append(tokens, t.Value())
	}
	return strings.Join(tokens, " ")
}

func (c *LogCluster) append(ts model.Time) {
	c.Size++
	c.Chunks.Add(ts)
}

func (c *LogCluster) merge(samples []*logproto.PatternSample) {
	c.Size += int(sumSize(samples))
	c.Chunks.merge(samples)
}

func (c *LogCluster) Iterator(from, through, step model.Time) iter.Iterator {
	return c.Chunks.Iterator(c.String(), from, through, step)
}

func (c *LogCluster) Samples() []*logproto.PatternSample {
	return c.Chunks.samples()
}

func (c *LogCluster) Prune(olderThan time.Duration) {
	c.Chunks.prune(olderThan)
	c.Size = c.Chunks.size()
}

func sumSize(samples []*logproto.PatternSample) int64 {
	var x int64
	for i := range samples {
		x += samples[i].Value
	}
	return x
}
