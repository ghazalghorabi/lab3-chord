package chord

import (
	"math/big"
	"sync"
)

// this struct will hold everything the chord node needs to remember about itself and the ring
type ChordNode struct {
	mu sync.Mutex //mu is a lock (mutex). The reason its needed is becuase there will be multiple go routines running at the same time:
	/* one for handling incoming network requests
	* one running stabilize periodically
	* one running fixFingers periodically
	* one running checkPredecessor periodically
	* without a mutex two goroutines might change Successors or Predecessor at the same time and corrupt the state */

	//identity of this node
	Self NodeInfo // self stores "who i am". Self containg the node's: chord ID, IP address and portNumber for ex: Self = { ID: 0xabc..., IP: "127.0.0.1", Port: 4170 }

	//chord pointers
	Predecessor *NodeInfo // may be nil, predecessor: means the node directly "behind" the ring. Its a pointer: *NodeInfo, a pointer can be nil (meaning "i dont know yet")
	// who is right before me? Predecessor == nil means: "unkown/none" otherwise it points to a real NodeInfo

	Successors []NodeInfo // successor list length= r. This is a list (slice) of nodes. For ex: Successors[0] is the immediate successor (the next node clockwise), successors[1], successors[2] are backup successors.
	//backup successors exist if the successor fails, you can quickly switch to the next one.
	//length=r, where r comes from the -r command line argument. Example with r = 4. Successors[0] = my next node, Successors[1] = next-next node, Successors[2] = next-next-next, Successors[3] = another backup

	Fingers []NodeInfo // finger table length = M, the length of M is usually (160). Each entry is a "shortcut" to help routing to find the correct node faster
	//finger[i] points to the successor of: Self.ID + 2^i (mod 2^M). Without fingers lookups are slow (you'd walk node-by-node). With fingers, lookup take about O(log N) hops

	R int // number of successors maintained, this stores the value of r from the command line -r
	// it tells how long the successors slice should be, example if the user runs -r 4 then R = 4 and you keep 4 successors

	nextFinger int
	files      map[string]string
}

// chordNode constructor. Creates a new Chord node object, fills in its identity (self), allocates space for successor list and finger table, sets predecessor to unkown (nil) and returns the node pointer
func NewChordNode(ip string, port int, id *big.Int, r int) *ChordNode { // it takes 4 inputs: ip string: the nodes Ip address, the nodes port (like 4170), the nodes chord ID (big int pointer), how many successors to store in the successor list
	//it returns *ChordNode which means a pointer to a chordNode struct you can create inside the function

	self := NodeInfo{ //creates a variable called self which is a type of NodeInfo
		ID: new(big.Int).Set(id), // This sets the ID field of self, id is a *big.Int(pointer to a big integer). new(big.Int) creates a brand new big integer object (initially 0). .Set(id) copies the numeric value from ID into the new big integer.
		//result: self.ID becomes a copy of the ID value

		IP:   ip,   //this sets the IP field of self to the value passed into the function
		Port: port, //sets the Port field of self to the value passed into the function
	}

	n := &ChordNode{ // this creates a ChordNode struct and stores its address (pointer) in n. &ChordNode {...} means: create a ChordNode value and return a pointer to it. Reason for pointer being returned is because multiple functions/goroutines can modify one shared node object
		Self:        self,                //sets the node's self field (who am i) to the NodeInfo above
		Predecessor: nil,                 //sets the predecessor pointer to nil, which in this context means: (dont know the predeccor yet)
		Successors:  make([]NodeInfo, r), //creates a slice (list) of NodeInfo values. Its length is r. This will hold the successor list: Successors[0] is the immediate successor. The rest of the successor are backups
		Fingers:     make([]NodeInfo, M), //creates the finger slice, length is M(which is set to 160).
		R:           r,                   //stores the configuration value r into the node struct.

		nextFinger: 0,
		files:      make(map[string]string),
	} // now n points to a chordNode that has: self= correct, predecessor = nil, successors slice allocated, fingers slice allocated, r stored

	return n // returns the pointer to the newly created node, the caller receives a usable *ChordNode

}

// in createRing() this node thinks it's the only node in the ring, its successor list is all itself, its finger table is all itself and predecessor is unkown (nil)
// createRing() is necessary because it establishes a valid chord ring when yor're the first node and it prevents nil states that would crash stabilization/routing. Its the correct initialization for one-node chord network.
func (n *ChordNode) CreateRing() { // a methid on ChordNode, n *ChordNode means the function works on a specific node object, createRing means: initialize this node as the first node in a brand new chord ring
	n.mu.Lock()         // locks the mutex so no other goroutine can change the node's fields at the same time. It prevents race conditions when changing: Predecessor, successors and fingers
	defer n.mu.Unlock() // defer = run this at the end of this function, no matter how the function exists the mutex will be unlocked

	n.Predecessor = nil //sets predeccesor to nil, nil means: "currently dot have/know a predecessor"

	for i := 0; i < n.R; i++ { // all successors points to self. This loop goes through every slot in the successor list. n.R is the number of successors you want to maintain. For ex if n.R == 4, this runs: i = 0,1,2,3
		n.Successors[i] = n.Self // put THIS node (n.self) into the successor slot i; meaning: “my successor (and all backup successors) is me.”; the reason for this is: if you're alone in the ring the next node clockwise is yourself
	}

	for i := 0; i < M; i++ { // all fingers point to self, this loop goes through the finger table entries. M is 160 (SHA-1 bit size). Ex. having 160 finger entries: Fingers[0]... Fingers[159]
		n.Fingers[i] = n.Self // set each finger entry to point to yourself; Meaning: “until I learn about other nodes, every routing shortcut just points to me.”, this makes the node’s routing still behave safely at startup.
	}

}

func (n *ChordNode) JoinRingWithKnownSuccessor(known NodeInfo) { // defines a method in ChordNode, known NodeInfo is the one node that already exsists in the ring. In the real program: known would be the node you connect with using: --ja and --jp

	n.mu.Lock()         // Locks the node state so nothing else changed it mid-update
	defer n.mu.Unlock() // run this at the end of the function

	//predecessor unkown at participate
	n.Predecessor = nil // sets the predecessor pointer to unkown. Because when you first participate; you dont know yet which node is immediatley before you. Chord learns predecessor information via notofy() during stabilization
	//so initially: predecessor = unknown; stabilization later fixes it

	//Successor list initailly all set to the known node
	for i := 0; i < n.R; i++ { // loops through the successor list, n.R is how many you keep from -r. It fills every slot with the same known mode. So after this: Successors[0] = known, Successors[1] = known ... Successors[r-1] = known
		n.Successors[i] = known // every node must always have a successor (or else it cant route anything)
	}

	//fingers initally also set to the known node
	// the finger table is used for routing jumps (closestPrecedingFinger). When you just participated you dont know any good short cuts, so you must still have vaild entries so you dont crash or route to nil
	for i := 0; i < M; i++ { // loops over every finger table entry (0..M-1, M=160)
		n.Fingers[i] = known // sets each finger entry to the same node; known: so: Fingers[0] = known, Fingers[1] = known and Fingers[159] = known
		//setting n.Fingers to know makes sure that the routing logic always has a real node to return. ficFingers() can gradually replace each finger with a correct value
	}
}

func (n *ChordNode) Successor() NodeInfo { // a method on ChordNode, its called like:L a.Successor() and returns a NodeInfo; the purpose is to give the current successor node:
	// in chord terms: Successors[0] is “the next node clockwise”. This is the main successor
	n.mu.Lock()         // Lock the mutex so no other goroutine can change the node state at the same time
	defer n.mu.Unlock() // the mutex needs to stay unlocked when the funtion ends (runned at the end)

	return n.Successors[0] // return the first entry in the successor list, in chord this is the immediate successor. So overall this means safley return the successor
}

func (n *ChordNode) SetSuccessor(s NodeInfo) { // a method that updates the successor, it takes one argument s; which is the node you want to set as successor. This doesnt return a value: it just updates state
	n.mu.Lock()
	defer n.mu.Unlock()

	n.Successors[0] = s //replace current successor with a new one: in chord terms; this is used when stabilization discovers a better successor. so this means: safley set my successor to this node

}
