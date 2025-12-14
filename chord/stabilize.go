package chord

func (n *ChordNode) Notify(candidate NodeInfo) { // this function runs when another node says: i might be your predecessor
	n.mu.Lock()
	defer n.mu.Unlock() // lock shared state, we are reading/writing n.Predecessor, so we lock!!

	//if predecessor is nil, it means that we dont have one yet, so the first node who notifies us vecomes oredecessor
	if n.Predecessor == nil { // if we have no predecessor yet: accept the candidate immediatle, first contact becomes predecessor,SO: We only replace predecessor if the candidate is “closer” to us on the ring;Is candidate.ID in (currentPredecessor.ID, myID) . If yes -> candidate is a better predecessor
		n.Predecessor = &candidate
		return
	}
	if idInInterval(candidate.ID, n.Predecessor.ID, n.Self.ID, false) { // is candidate closer to me than my current predecessor, this means:candidate ∈ (currentPredecessor, me)
		n.Predecessor = &candidate // if yes -> replace predecessor, chord always keeps the closest predecessor

	}

}

func (n *ChordNode) Stabilize() { // defines a method on the node n. it implements the chord protocol routine called stabilize, stabilize's purpose is to repair the ring pointers as nodes join/leave
	succ := n.Successor() // ask the local state "who is my successor right now", n.Successor returns n.Succesors[0]

	predecessorOfSucc, err := RPCGetPredecessor(succ.Address()) //this contacts successor over TCP and asks "who is the predecessor?"
	//it returns: predecessorOfSucc: a *NodeInfo (pointer) which might be nil and err: error if the call failed. The key idea is that theres maybe a node between you and your successor and that your successor knows it

	if err == nil && predecessorOfSucc != nil { // only continue if the rpc call worked (err == nil), successor actually has a predecessor (predecessorOfSucc != nil)
		// if successor has no predecessor, theres nothing to update from this step

		if idInInterval(predecessorOfSucc.ID, n.Self.ID, succ.ID, false) { // this checks if predessorofsucc is between the current id and the successor, false means end is not included
			//If successor’s predecessor sits between me and my successor, then that predecessor is a better (closer) successor for me
			//This is exactly how the ring “tightens up” after a join, ex: You think your successor is A, But A’s predecessor is B, If B is between you and A → your successor should actually be B
			n.SetSuccessor(*predecessorOfSucc) //update the local state, predecessorOfSucc is a pointer so *predecessorOfSucc is the actual NodeInfo value, you repair your successor pointer if it was skipping over a node
			succ = *predecessorOfSucc          // update the local variable succ too, because the code below uses succ.address() and we want to notify the new successor NOT the old one.
			//after choosing a new successor, you continoue stabilization by using the updated successor
		}
	}
	_ = RPCNotify(succ.Address(), n.Self) // call the successor and tell that it might be its predecessor
	//_= means ignore the error value
	// this is how the successor learns about you anc can set its predecssor correctly via Notify()
}
