package lb

import (
	"fmt"
)

// Strategy chooses one backend among the available candidates. Pick is
// always called with the pool lock held, so implementations may keep state
// without their own locking. cands is never empty.
type Strategy interface {
	Name() string
	Pick(cands []*Backend) *Backend
}

// NewStrategy returns a strategy by config name.
func NewStrategy(name string) (Strategy, error) {
	switch name {
	case "round_robin":
		return &RoundRobin{}, nil
	case "weighted":
		return &Weighted{}, nil
	case "least_conn":
		return &LeastConn{}, nil
	}
	return nil, fmt.Errorf("unknown strategy %q", name)
}

// RoundRobin cycles through candidates in order.
type RoundRobin struct{ next uint64 }

func (*RoundRobin) Name() string { return "round_robin" }
func (r *RoundRobin) Pick(c []*Backend) *Backend {
	b := c[r.next%uint64(len(c))]
	r.next++
	return b
}

// Weighted is nginx's "smooth weighted round robin". Every pick, each
// candidate's current weight grows by its configured weight; the largest
// wins and is reduced by the total. For weights 5,1,1 this yields
// a a b a c a a — spread out instead of a burst of five a's.
type Weighted struct{}

func (*Weighted) Name() string { return "weighted" }
func (*Weighted) Pick(c []*Backend) *Backend {
	total := 0
	var best *Backend
	for _, b := range c {
		w := b.Weight()
		b.currentWeight += w
		total += w
		if best == nil || b.currentWeight > best.currentWeight {
			best = b
		}
	}
	best.currentWeight -= total
	return best
}

// LeastConn sends the request to the backend with the fewest in-flight
// requests. Ties rotate so idle backends share load evenly.
type LeastConn struct{ next uint64 }

func (*LeastConn) Name() string { return "least_conn" }
func (l *LeastConn) Pick(c []*Backend) *Backend {
	start := int(l.next % uint64(len(c)))
	l.next++
	var best *Backend
	for i := range c {
		b := c[(start+i)%len(c)]
		if best == nil || b.Active() < best.Active() {
			best = b
		}
	}
	return best
}
