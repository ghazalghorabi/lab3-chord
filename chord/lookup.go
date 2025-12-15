package chord

import (
	"fmt"
	"math/big"
)

func (n *ChordNode) closestPrecedingFinger(key *big.Int) NodeInfo {
	n.mu.Lock()
	defer n.mu.Unlock()

	for i := M - 1; i >= 0; i-- {
		f := n.Fingers[i]
		if f.ID == nil {
			continue
		}

		if idInInterval(f.ID, n.Self.ID, key, false) {
			return f
		}
	}
	return n.Successors[0]
}

func (n *ChordNode) isResponsibleNext(key *big.Int) (NodeInfo, error) {
	succ := n.Successor()
	if succ.ID != nil && n.Self.ID.Cmp(succ.ID) == 0 {
		return n.Self, nil
	}

	current := n.Self

	for hop := 0; hop < 64; hop++ {
		var succ NodeInfo
		var ok bool

		if current.Address() == n.Self.Address() {
			succ, ok = n.isResponsibleNext(key)
			if ok {
				return succ, nil
			}
			current = n.closestPrecedingFinger(key)
			continue
		}

		next, done, err := RPCFindSuccessorStep(current.Address(), key)
		if err != nil {
			return NodeInfo{}, err
		}
		if done {
			return next, nil
		}
		current = next
	}

	return NodeInfo{}, fmt.Errorf("lookup exceeded hop limit")
}
