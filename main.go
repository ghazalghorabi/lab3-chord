package main

import (
	"bufio"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const mBits = 160 //(safety bound)Chord works on a circular identifier space of 2^m, SHA-1 ouputs 160 bits so the identifier fall in the range 0...2^160-1, so the programs mBits is the value m=160
// in rpcLookUpSuccessor (where the lookup is iterative (hops from node to node) it bounds the iterative lookup loop, so in a bad situation (bug, consistent routonh, partial failure) it might loop forever bit with mBits bound to 2^m, the loop stops at most 160 hops, then it gives up and returns nil). So this value prevents infinite loops/deadlock and keeps the node responsive even if routing is broken.
const fingerSize = 32 // this determines how many fingers exsist in newChordNode this allocates a finger table with 32 slots, so this initializes every finger table entry to point to the node itself, ensuring safe routing befire the node learns about other nodes

type NodeID = big.Int // node IDs and key IDs are large integers (160 bit) stored as big.Int

type NodeInfo struct { // starts a "package" of fields that describe a node, this how the chord node represnts in memory, (who it is + where to contact it)
	ID   *NodeID //stores the node's ID. Its a pointer so it can be nil sometimes for example when you dont know the ID yet
	IP   string  // stores the nodes IP address
	Port int     // stores the nodes port
}

type NodeInfoWire struct { // this exists only to send node information safley over the network, this struct is specifically for wire format (network transfer). So NodeInfoWire is a JSON-safe version of NodeInfo that converts the node ID into a string so it can be sent over the network
	ID   string `json:"id"`   // stores the node ID as a string, not a number, the string is a 40-character hex value for ex: "2f1a9c8e0d4b...". This tag `json:"id"` tells go: when converting to JSON call this field: id. So the JSON looks like: "id": "2f1a9c8e..."
	IP   string `json:"ip"`   // stores the node's IP address, IPs are already string: this tag: `json:"ip"` ensures that the JSON field is called "ip"
	Port int    `json:"port"` // stores the nodes port number
} // when sent over the network, a NodeInfoWire becomes:
// /*{ "id": "9c3a5b1e4d7f...",
//"ip": "128.8.126.63",
//  "port": 4170}*/

type FileRecord struct { // starts a new data type: one stored file inside the chord system; chord itself only finds where a key should live, this struct is what lets the system actually store data
	Name    string // stores the filename (ex. "Hello.txt), this lets the user remember what the file is called, not just its hash, so its used when printing or returning the file to the user
	Content string // stores the actual contents of the file. This is the real data the user care about, without this the storage system would store only names not files
} // this struct is necessary; because chord maps keys -> nodes, filerecord maps keys -> actual file data. This is what turns chord into a storage system instead of just a lookup system

type RPCRequest struct { // a request message sent from one node to another; this struct defines what a request looks like so it defines the format of incoming network requests
	Method string          `json:"method"` // stores what action the reciever should perform; for ex: "FindSuccessor", "PutFile", "GetFile", "Notify" etc. This tells the reciever which code path to run and its directly used in: switch req.Method { ... }
	Params json.RawMessage `json:"params"` // stores the data needed for that method as raw JSON, the reason for why its raw is because each method needs different parameters for ex: FindSuccesors needs an ID and PutFile needs key+name+content. So this lets one request format support many different RPC calls
} // without this struct the nodes wouldnt know what the user is asking and what data belongs to that request, this is the command envelope for all node to node communication

type RPCResponse struct { //This struct represent a reply sent back after handling an RPC request, every request gets exactly one response.
	//This type starts the response format definition
	Result interface{} `json:"result,omitempty"` // holds the successful return value, can be a node (NodeInfoWire), a file(FileRecord), a list, a string like "OK". This makes the reponse flexible so one response type works for all RPCs.
	// if Result is empty, it wont appear in json; so this keeps responses clean and avoids sending useles fields
	Error string `json:"error,omitempty"` // Holds an error message if something went wrong, it avoids failire to be reported without crashing and lets caller what to do next
} // this struct is important because ut separates succesw from failure and makes RPC communication predictable, this is how nodes answer each other safely

type ChordNode struct { // this struct represents everything the node knows and owns, without this struct the node effectivley has no memory
	mu          sync.Mutex // A lock to protect shared data; which is needed because multiple goroutines run at the same time for ex: server, Stabilize, foxFingers, user commands etc. This prevents corrupted state and race conditions
	Self        NodeInfo   // stores who this node is which includes its ID, IP and port. This is used in routing, comparisons and responses
	Predecessor *NodeInfo  // stores the node before this one in the ring, pointer allows nil(unknown). This is needed for stabilization, key migration and ring correctness
	Successors  []NodeInfo // list of nodes after this one in the ring, length is -r. This makes it fault tolerant: if successor dies, we already know the next one
	Fingers     []NodeInfo // fingers is a list (array/slice), each element is a NodeInfo so ID + IP + Port of some node, think of ot as the nodes shortcut address book.
	//without fingers, a node can only "walk" the ring using the successor: if you know youe successor, finding a key might require small hops: me -> next -> next -> next -> ...until the owner. With fingers, you can "jump" farther: me -> a node much closer to the target -> fewer hops total
	// so fingers are a performance feature, but they also help routing stay workable when the ring grows
	// in ideal chord, finger i should point to the node responsible for: selfID + 2^i. In this code, we do this for i= 0..31 (bcs fingerSize = 32). That means: finger[0] = successor of self+1, finger[1] = successor of self+2, finger[2] = successor of self+4, finger[3] = successor of self+8... finger[31] = successor of self+2³¹. These are increasing jump sizes
	nextFinger int                   // Tracks which finger to update next, so it allows fixFingers to update one finger at a time, evenly
	Files      map[string]FileRecord // stores files owned by this node, key = hashed filename (hex string); this is the actual distributed storage. So this is the node's local database of files. map [...] means a fast lookup table and map is like a dictionary/hash table. You can store and retrive items quickly O(1) (algo shit heheee) in average time
	//string is the key, FileRecord is the value (filename + file contents). So it behaves like: if "i give you a key string, you quickly give me the stored file"
	// The key you use is a string like: "9c3a5b1e4d7f... (40 hex chars)". That string comes from hashing the filename with SHA1 amd converting it to hex
}

// ========================Hashing + ID encoding helpers================================
// ======================Hash strings into Chord IDs and convert IDs to/from Hex ================================/

// it converts a string into a chord identifier (a 160 bit number) using SHA-1 so it takes a string like: "hello.txt" or "128.8.126.63:4170" and will return a chord ID
func hashStringToID(s string) *NodeID { // Defines a function that takes a string and return Chord ID (big number)
	h := sha1.Sum([]byte(s)) // turns the string into bytes ([]bytes(s)). Then hashes it using SHA1, result h is 20 bytes = 160 bits. It creates a consistent ID from a name (same input -> same ID every time)
	n := new(big.Int)        // creates an empty big integer object and prepares a number container to hold the 160 bit hash
	n.SetBytes(h[:])         // converts the 20 bytes of the SHA1 hash into a big integer, this makes the hash usable for ring comparisons and interval checks
	return n                 // returns the computed ID and it gives and gives the numeric key/node ID used by chord routing
} // Node ID when -i is not given: hashStringToID("ip:port") and file key for lookup/store: hashStringToId(filename)

func idToHex(id *NodeID) string { // function rhat turns an ID into a string, so this function turns a node or file ID into a fixed length hexadecimal string so it can be printed, stored and sent over the network consistentley
	return fmt.Sprintf("%040x", id) // formats the number as: hex(%x) and padded to 40 characters (%040). It gives a standard 40-hex-char form of IDs.
	//fmt.Sprintf means format something as a string, it does not print, it just returns a string
	// %x (hex formatting) means convert this number to a hexadecimal
	// hexadeximal: uses digits 0-9 and letters: a-f; its compact and standard for hashes for ex: deciamal:255 and hex: ff, so %x turns the big number into hex text
	//040 (padding and width), 40-> total width: 40 characters and 0-> pad with leading zeros if needed
	//the reason why its 40 charcters is because: SHA-1 = 160 bits; 1 hex digit = 4 bits, 160/4 = 40 hex digits; so every ID must be exactly 40 characters long
}

// it converts a node from the programs internal format into a network safe format so it can be sent over the network
// inside the program: NodeInfo and across the network (JSON): NodeInfoWire; this function does that conversion
func toWire(n NodeInfo) NodeInfoWire { // defines a function named toWire; its input is a NodeInfo (internal node representation) and it outputs a NodeInfoWire (network safe representation). So it establishes a clear boundary between internal state and network messages
	return NodeInfoWire{ // creates a new NodeInfoWire struct, starts building the version of the node that can be sent over TCP
		ID:   idToHex(n.ID), // takes the node's id and converts it to a 40 character hex string, it stores that string in the wire struct.
		IP:   n.IP,          // copies the IP address as it is, IPs are already strings so no conversion is nedded. So it preserves where the node can be contacted
		Port: n.Port,        // copies the port number, integers are safe in JSON, it preseves how to reach the node
	} // so toWire converts a nodes internal data into a JSON safe form so it can be sent to other nodes
}

func fromWire(w NodeInfoWire) *NodeInfo { // it converts a node recieved from the network back into the programs internal format. Network gives NodeInfoWire and the program needs NodeInfo. This function reverses what toWire does
	// so defines a function named fromWire and its input is NodeInfoWire (from JSON) and outputs a pointer to NodeInfo (internal format). So it brings network data back into usable chord data
	id, err := parseHexToID(w.ID) // takes the hex string ID and converts it back into a big integer. May fail if the string is invalid
	//it restores the numeric ID needed for the ring math
	if err != nil { // if the id cant be decoded, stop and return nil. this prevents crashed from malformed or corrupted network data
		return nil
	}
	return &NodeInfo{ // creates a new internal NodeInfo and uses the decoded ID, it reconstructs a proper node object the program can work with
		ID:   id,     // it assigns numeric id
		IP:   w.IP,   // ip address
		Port: w.Port, // and port number
	} // fully restores the nodes identity and location
} // fromWire converts node data received over the network into the internal format needed for chord routing

func parseHexToID(s string) (*NodeID, error) { // It converts a hexadecimal string back into a numeric Chord ID, this is the reverse of idToHex
	// so its input is a hex string and it outputs a nodeId and error if conversion fails. It explictly handles conversion and failure
	b, err := hex.DecodeString(s) // converts hex tect into raw bytes ex: 0a -> [10], So it turns readable text back into real binary data
	if err != nil {               // if the string is not valid hex: return an error
		return nil, err
	}
	n := new(big.Int) // creates a big integer container, so it prepares storage for a 160 bit number
	n.SetBytes(b)     // converts raw bytes into a number and makes the ID usable for comparisosns and interval checks
	return n, nil     // returns the ID and signals success
} // parseHexToID converts a hexadecimal string into a numeric chord ID so it can be used in routing calculations

// ============================Node initialization: Build safe starting chord state ====================================================
func newChordNode(self NodeInfo, r int) *ChordNode { // creates a new chord object, inputs self (this nodes ID/IP/Port), r(successor list size)
	//sets up all required state so the node can run
	n := &ChordNode{ // allocates a chordnode struct and stores it in n; now there's a node instance to fill in
		Self:        self,                         // saves this nodes own identity, this is used everywhere (routing, printing, comparisons)
		Predecessor: nil,                          // starts with no predecessor (unkown at startup)
		Successors:  make([]NodeInfo, r),          // creates a successor list with r slots, ut supports fault tolerance and ring maintenance
		Fingers:     make([]NodeInfo, fingerSize), // creates the finger table (32 entries), it enables faster routing once fixFingers fills it
		nextFinger:  -1,                           // sets the "next finger tp update" to -1, first fixFingers() increments it to 0 cleanly
		Files:       make(map[string]FileRecord),  // creates an empty storage map for files, this allows StoreFile/Lookup to work without nil map errors
	}
	// =============================== initialize successor list and finger table with safe defaults ===============================================

	// these loops initialize the successors list and finger table to point to the node itself, ensuring safe routing and correct behaviour before the node learns about other nodes
	for i := 0; i < r; i++ { // starts a loop counter at position 0, begins filling the successor list from the first slot;
		//i<r; repeat until all r successor slots are handled, this ensures that every successor entry is initialized
		// i++; move to the next successor slot
		n.Successors[i] = self // store this node (self) in successor slot i; this says the "successor is me"; this si the safest possible default when the node is alone or just starting, this prevents nil or invalid successors and allows node to: create a new ring safeley and survive before stabilization runs
	}

	for i := 0; i < fingerSize; i++ { //finger table initialization,
		// for i := 0; start the first finger entry, i < fingerSize; loop through all finger entries (32 in this code) and i++ move to the next finger slot
		n.Fingers[i] = self // set finger i to point to this node, which means i dont know any shortcuts yet so i route to myself; this prevents crashes or bad routing and makes lookup logic safe even befire fixFingers() runs, also allows node to function immediatley after startup
	}

	return n //sends the fully initialized node back to the caller; it confirms that the node now has: identity, succesors, fingers and storage
	// the node is now ready to create a ring, join a ring, accepts RPCS, store files
}

// ================================= Print current node state ====================================

func (n *ChordNode) PrintState() { // this function lets you see the nodes routings + files
	n.mu.Lock()         // lock state so it doesnt change while printing
	defer n.mu.Unlock() // unlock when the function ends

	fmt.Println("=== PrintState ===") // header text
	fmt.Printf("Self: id=%s ip=%s port=%d\n",

		idToHex(n.Self.ID), n.Self.IP, n.Self.Port) // print the ID/IP/port

	if n.Predecessor != nil {
		fmt.Printf("Predecessor: id=%s ip=%s port=%d\n",
			idToHex(n.Predecessor.ID), n.Predecessor.IP, n.Predecessor.Port)
	} else {
		fmt.Println("Predecessor: <nil>")
	}

	fmt.Println("Successors:")
	for i, s := range n.Successors {
		fmt.Printf("  [%d] id=%s ip=%s port=%d\n",
			i, idToHex(s.ID), s.IP, s.Port)
	}

	fmt.Println("Finger table:")
	for i, f := range n.Fingers {
		if idToHex(f.ID) == idToHex(n.Self.ID) {
			continue
		}
		fmt.Printf("  [%d] id=%s ip=%s port=%d\n",
			i, idToHex(f.ID), f.IP, f.Port)
	}

	fmt.Println("Files:")
	if len(n.Files) == 0 {
		fmt.Println("  (none)")
	} else {
		for key, rec := range n.Files {
			fmt.Printf("  key=%s name=%s len=%d\n", key, rec.Name, len(rec.Content))
		}
	}

	fmt.Println("=====================")
}

// ============================= RPC server Handler (respond to other nodes) =========================================================

func (n *ChordNode) handleConnection(conn net.Conn) {
	defer conn.Close()

	dec := json.NewDecoder(conn)
	enc := json.NewEncoder(conn)

	var req RPCRequest
	if err := dec.Decode(&req); err != nil {
		enc.Encode(RPCResponse{Error: "invalid request"})
		return
	}

	switch req.Method {
	// ============================== RPC methods (each case is one "feature") ================================================================
	case "Ping":
		enc.Encode(RPCResponse{Result: "PONG"})

	case "GetSuccessor":
		n.mu.Lock()
		succ := n.Successors[0]
		n.mu.Unlock()
		enc.Encode(RPCResponse{Result: toWire(succ)})

	case "GetPredecessor":
		n.mu.Lock()
		pred := n.Predecessor
		n.mu.Unlock()

		if pred == nil {
			enc.Encode(RPCResponse{Result: nil})
			return
		}
		enc.Encode(RPCResponse{Result: toWire(*pred)})

	case "Notify":
		var pw NodeInfoWire
		json.Unmarshal(req.Params, &pw)

		p := fromWire(pw)
		if p == nil {
			enc.Encode(RPCResponse{Error: "bad node info"})
			return
		}

		n.mu.Lock()
		remoteIP := conn.RemoteAddr().(*net.TCPAddr).IP.String()

		shouldUpdate := (n.Predecessor == nil ||
			inInterval(p.ID, n.Predecessor.ID, n.Self.ID, false)) &&
			remoteIP == p.IP

		if shouldUpdate {
			n.Predecessor = p
		}
		n.mu.Unlock()

		if shouldUpdate {
			n.migrateKeysFromSuccessor()
		}

		enc.Encode(RPCResponse{Result: "OK"})

	case "ClosestPrecedingFinger":
		var p struct{ ID string }
		json.Unmarshal(req.Params, &p)

		id, _ := parseHexToID(p.ID)
		f := n.closestPrecedingFinger(id)
		enc.Encode(RPCResponse{Result: toWire(*f)})

	case "FindSuccessor":
		var p struct{ ID string }
		json.Unmarshal(req.Params, &p)
		id, _ := parseHexToID(p.ID)

		n.mu.Lock()
		self := n.Self
		succ := n.Successors[0]
		n.mu.Unlock()

		if inInterval(id, self.ID, succ.ID, true) {
			enc.Encode(RPCResponse{Result: toWire(succ)})
			return
		}

		next := n.closestPrecedingFinger(id)

		if next.ID.Cmp(self.ID) == 0 {
			enc.Encode(RPCResponse{Result: toWire(succ)})
			return
		}

		enc.Encode(RPCResponse{Result: toWire(*next)})

	case "PutFile":
		var p struct {
			Key     string
			Name    string
			Content string
		}
		json.Unmarshal(req.Params, &p)
		n.mu.Lock()
		n.Files[p.Key] = FileRecord{
			Name:    p.Name,
			Content: p.Content,
		}
		n.mu.Unlock()

		enc.Encode(RPCResponse{Result: "OK"})

	case "GetFile":
		var p struct{ Key string }
		json.Unmarshal(req.Params, &p)

		n.mu.Lock()
		rec, ok := n.Files[p.Key]
		n.mu.Unlock()

		if !ok {
			enc.Encode(RPCResponse{Result: nil})
			return
		}
		enc.Encode(RPCResponse{Result: rec})

	case "GetRange":
		var p struct {
			Low  string
			High string
		}
		json.Unmarshal(req.Params, &p)

		result := make(map[string]FileRecord)
		n.mu.Lock()
		for k, v := range n.Files {
			kid, _ := parseHexToID(k)
			low, _ := parseHexToID(p.Low)
			high, _ := parseHexToID(p.High)
			if inInterval(kid, low, high, true) {
				result[k] = v
			}
		}
		n.mu.Unlock()
		enc.Encode(RPCResponse{Result: result})

	case "GetSuccessorList":
		n.mu.Lock()
		list := make([]NodeInfoWire, len(n.Successors))
		for i, s := range n.Successors {
			list[i] = toWire(s)
		}
		n.mu.Unlock()
		enc.Encode(RPCResponse{Result: list})

	case "DeleteKeys":
		var p struct {
			Keys []string
		}
		json.Unmarshal(req.Params, &p)

		n.mu.Lock()
		allowed := n.Predecessor != nil &&
			n.Predecessor.IP == conn.RemoteAddr().(*net.TCPAddr).IP.String() &&
			n.Predecessor.Port == conn.RemoteAddr().(*net.TCPAddr).Port

		if allowed {
			for _, k := range p.Keys {
				delete(n.Files, k)
			}
		}
		n.mu.Unlock()

		if !allowed {
			enc.Encode(RPCResponse{Error: "unauthorized"})
			return
		}

		enc.Encode(RPCResponse{Result: "OK"})

	default:
		enc.Encode(RPCResponse{Error: "unknown method"})
	}
}

// ================================ Stabilize ring (keep successor/links correct) ==============================================================

func (n *ChordNode) stabilize() {
	n.mu.Lock()
	succ := n.Successors[0]
	n.mu.Unlock()

	if err := tryPing(succ); err != nil {
		n.mu.Lock()
		for i := 0; i < len(n.Successors)-1; i++ {
			n.Successors[i] = n.Successors[i+1]
		}
		n.Successors[len(n.Successors)-1] = n.Self
		n.mu.Unlock()
		return
	}

	x := rpcGetPredecessor(succ)
	if x != nil {
		n.mu.Lock()
		if inInterval(x.ID, n.Self.ID, n.Successors[0].ID, false) {
			n.Successors[0] = *x
		}
		n.mu.Unlock()
	}

	n.mu.Lock()
	succ = n.Successors[0]
	n.mu.Unlock()

	other := rpcGetSuccessorList(succ)

	if other != nil {
		n.mu.Lock()
		for i := 1; i < len(n.Successors) && i-1 < len(other); i++ {
			n.Successors[i] = other[i-1]
		}
		n.mu.Unlock()
	}

	n.mu.Lock()
	succ = n.Successors[0]
	n.mu.Unlock()

	addr := fmt.Sprintf("%s:%d", succ.IP, succ.Port)
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return
	}
	conn.SetDeadline(time.Now().Add(2 * time.Second))

	defer conn.Close()

	enc := json.NewEncoder(conn)
	enc.Encode(RPCRequest{
		Method: "Notify",
		Params: mustJSON(toWire(n.Self)),
	})
}

// ============================ JSON helper (convert any value to raw json) ========================================================
func mustJSON(v interface{}) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}

// ============================ run stabilize periodically =============================================================================
func (n *ChordNode) stabilizeLoop(interval time.Duration) {
	for {
		time.Sleep(interval)
		n.stabilize()
		//fmt.Println("[stabilize] done")
	}
}

// ============================ check predecessor is alive (failure detection) =================================================================
func (n *ChordNode) checkPredecessor() {
	n.mu.Lock()
	pred := n.Predecessor
	n.mu.Unlock()

	if pred == nil {
		return
	}

	addr := fmt.Sprintf("%s:%d", pred.IP, pred.Port)
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		n.mu.Lock()
		n.Predecessor = nil
		n.mu.Unlock()
		return
	}
	conn.SetDeadline(time.Now().Add(2 * time.Second))
	conn.Close()
}

// =============================== Run predecessor, check periodically =============================================================================
func (n *ChordNode) checkPredecessorLoop(interval time.Duration) {
	for {
		time.Sleep(interval)
		n.checkPredecessor()
		//fmt.Println("[checkPredecessor] done")
	}
}

// =============================== Fic one finger entry (update routing shortcut) =============================================================================

func (n *ChordNode) fixFingers() {
	n.mu.Lock()
	n.nextFinger = (n.nextFinger + 1) % fingerSize

	start := new(big.Int)
	twoPow := new(big.Int).Exp(big.NewInt(2), big.NewInt(int64(n.nextFinger)), nil)
	start.Add(n.Self.ID, twoPow)
	start.Mod(start, new(big.Int).Exp(big.NewInt(2), big.NewInt(160), nil))
	n.mu.Unlock()

	succ := n.findSuccessor(start)
	if succ != nil {
		n.mu.Lock()
		n.Fingers[n.nextFinger] = *succ
		n.mu.Unlock()
	}
}

// =============================== Run FixFingers periodically =============================================================================
func (n *ChordNode) fixFingersLoop(interval time.Duration) {
	for {
		time.Sleep(interval)
		n.fixFingers()
		//fmt.Println("[fixFingers] done")
	}
}

// =============================== Find the node responsible for an ID (start Lookup from the self node) =============================================================================
func (n *ChordNode) findSuccessor(id *NodeID) *NodeInfo {
	n.mu.Lock()
	start := n.Self
	n.mu.Unlock()
	return rpcLookupSuccessor(start, id)
}

// =============================== Run math: check if x is between a and b on a circle =============================================================================
func inInterval(x, a, b *NodeID, inclusiveEnd bool) bool {
	mod := new(big.Int).Exp(big.NewInt(2), big.NewInt(160), nil)

	xN := new(big.Int).Mod(x, mod)
	aN := new(big.Int).Mod(a, mod)
	bN := new(big.Int).Mod(b, mod)

	if aN.Cmp(bN) < 0 {
		if inclusiveEnd {
			return xN.Cmp(aN) > 0 && xN.Cmp(bN) <= 0
		}
		return xN.Cmp(aN) > 0 && xN.Cmp(bN) < 0
	}

	if inclusiveEnd {
		return xN.Cmp(aN) > 0 || xN.Cmp(bN) <= 0
	}
	return xN.Cmp(aN) > 0 || xN.Cmp(bN) < 0
}

// =============================== Choose best next hop using finger table =============================================================================
func (n *ChordNode) closestPrecedingFinger(target *NodeID) *NodeInfo {
	n.mu.Lock()
	defer n.mu.Unlock()

	for i := len(n.Fingers) - 1; i >= 0; i-- {
		f := n.Fingers[i]
		if f.ID != nil && inInterval(f.ID, n.Self.ID, target, false) {
			out := f
			return &out
		}
	}

	out := n.Self
	return &out
}

// =============================== RPC client: ask a node for its predecessor =============================================================================

func rpcGetPredecessor(target NodeInfo) *NodeInfo {
	addr := fmt.Sprintf("%s:%d", target.IP, target.Port)
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return nil
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(2 * time.Second))

	enc := json.NewEncoder(conn)
	dec := json.NewDecoder(conn)

	enc.Encode(RPCRequest{Method: "GetPredecessor"})

	var resp RPCResponse
	if err := dec.Decode(&resp); err != nil || resp.Error != "" {
		return nil
	}

	if resp.Result == nil {
		return nil
	}

	b, _ := json.Marshal(resp.Result)
	var pw NodeInfoWire
	if err := json.Unmarshal(b, &pw); err != nil {
		return nil
	}
	return fromWire(pw)

}

// =============================== RPC client: ask a node to route (findSuccessor/ next hop) =============================================================================
func rpcFindSuccessor(target NodeInfo, id *NodeID) *NodeInfo {
	addr := fmt.Sprintf("%s:%d", target.IP, target.Port)
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return nil
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(2 * time.Second))

	enc := json.NewEncoder(conn)
	dec := json.NewDecoder(conn)

	enc.Encode(RPCRequest{
		Method: "FindSuccessor",
		Params: mustJSON(struct{ ID string }{idToHex(id)}),
	})

	var resp RPCResponse
	if err := dec.Decode(&resp); err != nil || resp.Error != "" {
		return nil
	}

	b, _ := json.Marshal(resp.Result)
	var pw NodeInfoWire
	if err := json.Unmarshal(b, &pw); err != nil {
		return nil
	}
	return fromWire(pw)

}

// =============================== RPC client: store a file on a remote node =============================================================================
func rpcPutFile(target NodeInfo, key, name, content string) bool {
	addr := fmt.Sprintf("%s:%d", target.IP, target.Port)
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return false
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(2 * time.Second))

	enc := json.NewEncoder(conn)
	dec := json.NewDecoder(conn)

	enc.Encode(RPCRequest{
		Method: "PutFile",
		Params: mustJSON(struct {
			Key     string
			Name    string
			Content string
		}{
			Key:     key,
			Name:    name,
			Content: content,
		}),
	})

	var resp RPCResponse
	if err := dec.Decode(&resp); err != nil || resp.Error != "" {
		return false
	}

	return true
}

// =============================== RPC client: fetch a file from a remote node =============================================================================
func rpcGetFile(target NodeInfo, key string) *FileRecord {
	addr := fmt.Sprintf("%s:%d", target.IP, target.Port)
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return nil
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(2 * time.Second))

	enc := json.NewEncoder(conn)
	dec := json.NewDecoder(conn)

	enc.Encode(RPCRequest{
		Method: "GetFile",
		Params: mustJSON(struct{ Key string }{key}),
	})

	var resp RPCResponse
	if err := dec.Decode(&resp); err != nil || resp.Error != "" {
		return nil
	}

	if resp.Result == nil {
		return nil
	}

	b, _ := json.Marshal(resp.Result)
	var rec FileRecord
	json.Unmarshal(b, &rec)
	return &rec
}

// =============================== RPC client: fetch all keys in an interval (for migration) =============================================================================
func rpcGetRange(target NodeInfo, lowHex, highHex string) map[string]FileRecord {
	addr := fmt.Sprintf("%s:%d", target.IP, target.Port)
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return nil
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(2 * time.Second))

	enc := json.NewEncoder(conn)
	dec := json.NewDecoder(conn)

	enc.Encode(RPCRequest{
		Method: "GetRange",
		Params: mustJSON(struct {
			Low  string
			High string
		}{
			Low:  lowHex,
			High: highHex,
		}),
	})

	var resp RPCResponse
	if err := dec.Decode(&resp); err != nil || resp.Error != "" {
		return nil
	}

	if resp.Result == nil {
		return nil
	}

	b, _ := json.Marshal(resp.Result)
	var result map[string]FileRecord
	json.Unmarshal(b, &result)
	return result
}

// =============================== RPC client: fetch a node's successor list =============================================================================

func rpcGetSuccessorList(target NodeInfo) []NodeInfo {
	addr := fmt.Sprintf("%s:%d", target.IP, target.Port)
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return nil
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(2 * time.Second))

	enc := json.NewEncoder(conn)
	dec := json.NewDecoder(conn)

	enc.Encode(RPCRequest{
		Method: "GetSuccessorList",
		Params: json.RawMessage(`{}`),
	})

	var resp RPCResponse
	if err := dec.Decode(&resp); err != nil || resp.Error != "" {
		return nil
	}

	b, _ := json.Marshal(resp.Result)
	var wires []NodeInfoWire
	if err := json.Unmarshal(b, &wires); err != nil {
		return nil
	}

	list := make([]NodeInfo, 0, len(wires))
	for _, w := range wires {
		n := fromWire(w)
		if n != nil {
			list = append(list, *n)
		}
	}
	return list

}

// =============================== RPC client: ping a node (check if alive) =============================================================================

func tryPing(n NodeInfo) error {
	addr := fmt.Sprintf("%s:%d", n.IP, n.Port)
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(2 * time.Second))

	enc := json.NewEncoder(conn)
	dec := json.NewDecoder(conn)

	enc.Encode(RPCRequest{Method: "Ping"})

	var resp RPCResponse
	return dec.Decode(&resp)
}

// =============================== TPC server: listen and handle connections =============================================================================

func (n *ChordNode) ListenAndServe() error {
	ln, err := net.Listen("tcp", fmt.Sprintf("%s:%d", n.Self.IP, n.Self.Port))
	if err != nil {
		return err
	}
	for {
		conn, err := ln.Accept()
		if err == nil {
			go n.handleConnection(conn)
		}
	}
}

// =============================== Key migratition: copy keys from successors that now belong to me =============================================================================
func (n *ChordNode) migrateKeysFromSuccessor() {
	n.mu.Lock()
	pred := n.Predecessor
	self := n.Self
	succ := n.Successors[0]
	n.mu.Unlock()

	if pred == nil || succ.ID.Cmp(self.ID) == 0 {
		return
	}

	keys := rpcGetRange(succ, idToHex(pred.ID), idToHex(self.ID))
	if keys == nil || len(keys) == 0 {
		return
	}

	var keyList []string
	n.mu.Lock()
	for k, v := range keys {
		n.Files[k] = v
		keyList = append(keyList, k)
	}
	n.mu.Unlock()

	addr := fmt.Sprintf("%s:%d", succ.IP, succ.Port)
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(2 * time.Second))

	enc := json.NewEncoder(conn)
	enc.Encode(RPCRequest{
		Method: "DeleteKeys",
		Params: mustJSON(struct{ Keys []string }{keyList}),
	})
}

// =============================== Iterative lookup: walk node-to-node until sucessor found =============================================================================
func rpcLookupSuccessor(start NodeInfo, id *NodeID) *NodeInfo {
	cur := start

	for i := 0; i < mBits; i++ {
		resp := rpcFindSuccessor(cur, id)
		if resp == nil {
			return nil
		}

		if cur.ID == nil {
			cur = *resp
			continue
		}

		if inInterval(id, cur.ID, resp.ID, true) {
			return resp
		}

		cur = *resp
	}
	return nil
}

// =============================== Program startup: parse flags, create/join ring, run loops, read commands =============================================================================

func main() {
	ip := flag.String("a", "", "IP address to bind and advertise")
	port := flag.Int("p", 0, "Port to bind and listen on")

	joinIP := flag.String("ja", "", "IP of node to join")
	joinPort := flag.Int("jp", 0, "Port of node to join")

	ts := flag.Int("ts", -1, "Time between stabilize() calls (ms)")
	tff := flag.Int("tff", -1, "Time between fix_fingers() calls (ms)")
	tcp := flag.Int("tcp", -1, "Time between check_predecessor() calls (ms)")

	r := flag.Int("r", -1, "Number of successors to maintain")
	manualID := flag.String("i", "", "Optional manual node ID (40 hex chars)")

	flag.Parse()

	if *ip == "" || *port == 0 {
		fmt.Println("Error: -a <ip> and -p <port> are required.")
		os.Exit(1)
	}

	if *ts < 1 || *ts > 60000 || *tff < 1 || *tff > 60000 || *tcp < 1 || *tcp > 60000 {
		fmt.Println("Error: --ts, --tff, --tcp must be in [1,60000].")
		os.Exit(1)
	}

	if *r < 1 || *r > 32 {
		fmt.Println("Error: -r must be in [1,32].")
		os.Exit(1)
	}

	joining := false
	if *joinIP != "" || *joinPort != 0 {
		if *joinIP == "" || *joinPort == 0 {
			fmt.Println("Error: both --ja and --jp must be specified to join a ring.")
			os.Exit(1)
		}
		joining = true
	}

	fmt.Println("=== Chord Node Configuration ===")
	fmt.Println("IP:", *ip)
	fmt.Println("Port:", *port)
	fmt.Println("TS:", *ts)
	fmt.Println("TFF:", *tff)
	fmt.Println("TCP:", *tcp)
	fmt.Println("Successor List Size:", *r)

	if joining {
		fmt.Println("Mode: JOIN existing ring at", *joinIP, *joinPort)
	} else {
		fmt.Println("Mode: CREATE new ring")
	}

	if *manualID != "" {
		fmt.Println("Manual ID:", *manualID)
	} else {
		fmt.Println("Manual ID: (none)")
	}

	fmt.Println("================================")

	var myID *NodeID
	if *manualID != "" {
		if len(*manualID) != 40 {
			fmt.Println("Error: manual ID must be exactly 40 hex characters")
			os.Exit(1)
		}

		_, err := hex.DecodeString(*manualID)
		if err != nil {
			fmt.Println("Error: manual ID must be hexadecimal")
			os.Exit(1)
		}

		id, _ := parseHexToID(*manualID)
		myID = id
	} else {
		myID = hashStringToID(fmt.Sprintf("%s:%d", *ip, *port))
	}

	fmt.Println("Computed node ID:", idToHex(myID))

	selfInfo := NodeInfo{
		ID:   myID,
		IP:   *ip,
		Port: *port,
	}
	node := newChordNode(selfInfo, *r)
	if joining {
		bootstrap := NodeInfo{
			ID:   nil,
			IP:   *joinIP,
			Port: *joinPort,
		}

		succ := rpcLookupSuccessor(bootstrap, myID)
		if succ == nil {
			fmt.Println("Join failed: could not find successor")
			os.Exit(1)
		}

		node.mu.Lock()
		for i := range node.Successors {
			node.Successors[i] = *succ
		}
		for i := range node.Fingers {
			node.Fingers[i] = *succ
		}
		node.mu.Unlock()

		fmt.Println("Joined ring. My successor is:", idToHex(succ.ID), succ.IP, succ.Port)
	} else {
		fmt.Println("Created new ring (successor = self)")
	}

	fmt.Println("Node created. Initial state:")
	node.PrintState()

	go func() {
		if err := node.ListenAndServe(); err != nil {
			fmt.Println("Listen error:", err)
			os.Exit(1)
		}
	}()

	go node.stabilizeLoop(time.Duration(*ts) * time.Millisecond)
	go node.fixFingersLoop(time.Duration(*tff) * time.Millisecond)
	go node.checkPredecessorLoop(time.Duration(*tcp) * time.Millisecond)

	fmt.Println("Ready for commands. Type 'PrintState' or 'quit'.")
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		parts := strings.Fields(line)
		cmd := parts[0]

		switch cmd {
		case "quit":
			fmt.Println("Exiting...")
			return

		case "PrintState":
			node.PrintState()

		case "StoreFile":
			if len(parts) != 2 {
				fmt.Println("Usage: StoreFile <path>")
				continue
			}

			path := parts[1]
			data, err := os.ReadFile(path)
			if err != nil {
				fmt.Println("Error:", err)
				continue
			}

			name := filepath.Base(path)
			keyID := hashStringToID(name)
			keyHex := idToHex(keyID)

			succ := node.findSuccessor(keyID)
			if succ == nil {
				fmt.Println("Error: successor lookup failed")
				continue
			}

			if succ.ID.Cmp(node.Self.ID) == 0 {
				node.mu.Lock()
				node.Files[keyHex] = FileRecord{Name: name, Content: string(data)}
				node.mu.Unlock()
				fmt.Println("Stored locally")
			} else {
				ok := rpcPutFile(*succ, keyHex, name, string(data))
				if ok {
					fmt.Println("Stored on node", succ.IP, succ.Port)
				} else {
					fmt.Println("Remote store failed")
				}
			}

		case "Lookup":
			if len(parts) != 2 {
				fmt.Println("Usage: Lookup <filename>")
				continue
			}

			name := parts[1]
			keyID := hashStringToID(name)
			keyHex := idToHex(keyID)

			succ := node.findSuccessor(keyID)
			if succ == nil {
				fmt.Println("Lookup failed: no successor")
				continue
			}

			fmt.Printf("Owner: %s %s %d\n",
				idToHex(succ.ID), succ.IP, succ.Port)

			if succ.ID.Cmp(node.Self.ID) == 0 {
				node.mu.Lock()
				rec, ok := node.Files[keyHex]
				node.mu.Unlock()
				if !ok {
					fmt.Println("File not found locally")
					continue
				}
				fmt.Println(rec.Content)
			} else {
				rec := rpcGetFile(*succ, keyHex)
				if rec == nil {
					fmt.Println("File not found on remote node")
					continue
				}
				fmt.Println(rec.Content)
			}

		default:
			fmt.Println("Unknown command. Use: PrintState, StoreFile, Lookup, quit")
		}
	}

}
