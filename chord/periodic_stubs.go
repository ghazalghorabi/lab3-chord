package chord

import "log"

func (n *ChordNode) FixFingers() {
	//logic fixfingers later

	log.Printf("[node %s] FixFingers tick", n.Self.Address())
}

func (n *ChordNode) CheckPredecessor() {
	//real check predesccer logic later
	log.Printf("[node %s] CheckPredecessor tick", n.Self.Address())
}
