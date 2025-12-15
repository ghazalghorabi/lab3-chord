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
	"strconv"
	"strings"
	"sync"
	"time"
)

const mBits = 160 // size of identifier space (SHA1 → 160 bits)

// NodeID is just a big integer
type NodeID = big.Int

// NodeInfo holds basic info about a node
type NodeInfo struct {
	ID   *NodeID
	IP   string
	Port int
}

// Wire (JSON) representation
type NodeInfoWire struct {
	ID   string `json:"id"`
	IP   string `json:"ip"`
	Port int    `json:"port"`
}

type FileRecord struct {
	Name    string
	Content string
}

type RPCRequest struct {
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
}

type RPCResponse struct {
	Result interface{} `json:"result,omitempty"`
	Error  string      `json:"error,omitempty"`
}

// ChordNode holds all state for our node (we'll fill it later)
type ChordNode struct {
	mu          sync.Mutex
	Self        NodeInfo
	Predecessor *NodeInfo
	Successors  []NodeInfo
	Fingers     []NodeInfo
	nextFinger  int
	Files       map[string]FileRecord // key = hex-encoded ID
}

// hashStringToID takes a string and returns a 160-bit ID (SHA1)
func hashStringToID(s string) *NodeID {
	h := sha1.Sum([]byte(s)) // 20 bytes
	n := new(big.Int)
	n.SetBytes(h[:])
	return n
}

// idToHex converts an ID to a 40-char hex string
func idToHex(id *NodeID) string {
	// Big int to bytes → hex
	return fmt.Sprintf("%040x", id)
}

func toWire(n NodeInfo) NodeInfoWire {
	return NodeInfoWire{
		ID:   idToHex(n.ID),
		IP:   n.IP,
		Port: n.Port,
	}
}

func fromWire(w NodeInfoWire) *NodeInfo {
	id, err := parseHexToID(w.ID)
	if err != nil {
		return nil
	}
	return &NodeInfo{
		ID:   id,
		IP:   w.IP,
		Port: w.Port,
	}
}

// parseHexToID parses a 40-hex-digit string into an ID
func parseHexToID(s string) (*NodeID, error) {
	b, err := hex.DecodeString(s)
	if err != nil {
		return nil, err
	}
	n := new(big.Int)
	n.SetBytes(b)
	return n, nil
}

func newChordNode(self NodeInfo, r int) *ChordNode {
	n := &ChordNode{
		Self:        self,
		Predecessor: nil,
		Successors:  make([]NodeInfo, r),
		Fingers:     make([]NodeInfo, mBits),
		Files:       make(map[string]FileRecord),
	}

	for i := 0; i < r; i++ {
		n.Successors[i] = self
	}
	for i := 0; i < mBits; i++ {
		n.Fingers[i] = self
	}

	return n
}

func (n *ChordNode) PrintState() {
	n.mu.Lock()
	defer n.mu.Unlock()
	fmt.Println("=== PrintState ===")
	fmt.Printf("Self: id=%s ip=%s port=%d\n",
		idToHex(n.Self.ID), n.Self.IP, n.Self.Port)

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

	fmt.Println("==================")
}

// handleConnection handles one incoming TCP connection from another node or client.
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
		shouldUpdate := n.Predecessor == nil ||
			inInterval(p.ID, n.Predecessor.ID, n.Self.ID, false)
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
		succ := n.findSuccessor(id)
		if succ == nil {
			enc.Encode(RPCResponse{Result: nil})
			return
		}
		enc.Encode(RPCResponse{Result: toWire(*succ)})

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
		for _, k := range p.Keys {
			delete(n.Files, k)
		}
		n.mu.Unlock()

		enc.Encode(RPCResponse{Result: "OK"})

	default:
		enc.Encode(RPCResponse{Error: "unknown method"})
	}
}

func (n *ChordNode) stabilize() {
	n.mu.Lock()
	succ := n.Successors[0]
	n.mu.Unlock()

	// 1. Check successor liveness WITHOUT holding lock
	if err := tryPing(succ); err != nil {
		n.mu.Lock()
		for i := 0; i < len(n.Successors)-1; i++ {
			n.Successors[i] = n.Successors[i+1]
		}
		n.Successors[len(n.Successors)-1] = n.Self
		n.mu.Unlock()
		return
	}

	// 2. Ask successor for its predecessor
	x := rpcGetPredecessor(succ)
	if x != nil {
		n.mu.Lock()
		if inInterval(x.ID, n.Self.ID, n.Successors[0].ID, false) {
			n.Successors[0] = *x
		}
		n.mu.Unlock()
	}

	// 3. Refresh successor list
	other := rpcGetSuccessorList(succ)
	if other != nil {
		n.mu.Lock()
		for i := 1; i < len(n.Successors) && i-1 < len(other); i++ {
			n.Successors[i] = other[i-1]
		}
		n.mu.Unlock()
	}

	// 4. Notify successor
	n.mu.Lock()
	succ = n.Successors[0]
	n.mu.Unlock()

	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", succ.IP, succ.Port))
	if err != nil {
		return
	}
	defer conn.Close()

	enc := json.NewEncoder(conn)
	enc.Encode(RPCRequest{
		Method: "Notify",
		Params: mustJSON(toWire(n.Self)),
	})
}

func mustJSON(v interface{}) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}

func (n *ChordNode) stabilizeLoop(interval time.Duration) {
	for {
		time.Sleep(interval)
		n.stabilize()
		fmt.Println("[stabilize] done")
	}
}

func (n *ChordNode) checkPredecessor() {
	n.mu.Lock()
	pred := n.Predecessor
	n.mu.Unlock()

	if pred == nil {
		return
	}

	addr := fmt.Sprintf("%s:%d", pred.IP, pred.Port)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		n.mu.Lock()
		n.Predecessor = nil
		n.mu.Unlock()
		return
	}
	conn.Close()
}

func (n *ChordNode) checkPredecessorLoop(interval time.Duration) {
	for {
		time.Sleep(interval)
		n.checkPredecessor()
		fmt.Println("[checkPredecessor] done")
	}
}

func (n *ChordNode) fixFingers() {
	n.mu.Lock()
	n.nextFinger = (n.nextFinger + 1) % mBits

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

func (n *ChordNode) fixFingersLoop(interval time.Duration) {
	for {
		time.Sleep(interval)
		n.fixFingers()
		fmt.Println("[fixFingers] done")
	}
}

func (n *ChordNode) findSuccessor(id *NodeID) *NodeInfo {
	n.mu.Lock()
	onlyNode := n.Successors[0].ID.Cmp(n.Self.ID) == 0
	n.mu.Unlock()

	//Single-node ring shortcut
	if onlyNode {
		return &n.Self
	}

	cur := n.Self
	var lastSucc *NodeInfo

	for hops := 0; hops < mBits; hops++ {
		succ := rpcGetSuccessor(cur)
		if succ == nil {
			return lastSucc
		}
		lastSucc = succ

		if inInterval(id, cur.ID, succ.ID, true) {
			return succ
		}

		cpf := rpcClosestPrecedingFinger(cur, id)
		if cpf == nil || cpf.ID.Cmp(cur.ID) == 0 {
			return succ
		}

		cur = *cpf
	}
	return lastSucc
}

// inInterval returns true if x ∈ (a, b] or (a, b), depending on inclusiveEnd.
func inInterval(x, a, b *NodeID, inclusiveEnd bool) bool {
	mod := new(big.Int).Exp(big.NewInt(2), big.NewInt(160), nil)

	xN := new(big.Int).Mod(x, mod)
	aN := new(big.Int).Mod(a, mod)
	bN := new(big.Int).Mod(b, mod)

	// Case 1: normal order (a < b)
	if aN.Cmp(bN) < 0 {
		if inclusiveEnd {
			// (a, b]
			return xN.Cmp(aN) > 0 && xN.Cmp(bN) <= 0
		}
		// (a, b)
		return xN.Cmp(aN) > 0 && xN.Cmp(bN) < 0
	}

	// Case 2: wrap-around (a > b)
	if inclusiveEnd {
		return xN.Cmp(aN) > 0 || xN.Cmp(bN) <= 0
	}
	return xN.Cmp(aN) > 0 || xN.Cmp(bN) < 0
}

// closestPrecedingFinger returns the closest finger preceding the target ID.
func (n *ChordNode) closestPrecedingFinger(target *NodeID) *NodeInfo {
	for i := len(n.Fingers) - 1; i >= 0; i-- {
		if n.Fingers[i].ID != nil &&
			inInterval(n.Fingers[i].ID, n.Self.ID, target, false) {
			return &n.Fingers[i]
		}
	}
	return &n.Self
}

func rpcGetPredecessor(target NodeInfo) *NodeInfo {
	conn, err := net.Dial("tcp", target.IP+":"+strconv.Itoa(target.Port))
	if err != nil {
		return nil
	}
	defer conn.Close()

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

func rpcClosestPrecedingFinger(target NodeInfo, id *NodeID) *NodeInfo {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", target.IP, target.Port))
	if err != nil {
		return nil
	}
	defer conn.Close()

	enc := json.NewEncoder(conn)
	dec := json.NewDecoder(conn)

	enc.Encode(RPCRequest{
		Method: "ClosestPrecedingFinger",
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

func rpcGetSuccessor(target NodeInfo) *NodeInfo {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", target.IP, target.Port))
	if err != nil {
		return nil
	}
	defer conn.Close()

	enc := json.NewEncoder(conn)
	dec := json.NewDecoder(conn)

	// Send request
	enc.Encode(RPCRequest{
		Method: "GetSuccessor",
		Params: json.RawMessage(`{}`),
	})

	// Read response
	var resp RPCResponse
	if err := dec.Decode(&resp); err != nil || resp.Error != "" {
		return nil
	}

	// Decode result into NodeInfo
	data, _ := json.Marshal(resp.Result)
	var pw NodeInfoWire
	if err := json.Unmarshal(data, &pw); err != nil {
		return nil
	}
	return fromWire(pw)

}

func rpcFindSuccessor(target NodeInfo, id *NodeID) *NodeInfo {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", target.IP, target.Port))
	if err != nil {
		return nil
	}
	defer conn.Close()

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

func rpcPutFile(target NodeInfo, key, name, content string) bool {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", target.IP, target.Port))
	if err != nil {
		return false
	}
	defer conn.Close()

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

func rpcGetFile(target NodeInfo, key string) *FileRecord {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", target.IP, target.Port))
	if err != nil {
		return nil
	}
	defer conn.Close()

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

func rpcGetRange(target NodeInfo, lowHex, highHex string) map[string]FileRecord {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", target.IP, target.Port))
	if err != nil {
		return nil
	}
	defer conn.Close()

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

func rpcGetSuccessorList(target NodeInfo) []NodeInfo {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", target.IP, target.Port))
	if err != nil {
		return nil
	}
	defer conn.Close()

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

func tryPing(n NodeInfo) error {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", n.IP, n.Port))
	if err != nil {
		return err
	}
	defer conn.Close()

	enc := json.NewEncoder(conn)
	dec := json.NewDecoder(conn)

	enc.Encode(RPCRequest{Method: "Ping"})

	var resp RPCResponse
	return dec.Decode(&resp)
}

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

	// Store locally
	var keyList []string
	n.mu.Lock()
	for k, v := range keys {
		n.Files[k] = v
		keyList = append(keyList, k)
	}
	n.mu.Unlock()

	// Tell successor to delete transferred keys
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", succ.IP, succ.Port))
	if err != nil {
		return
	}
	defer conn.Close()

	enc := json.NewEncoder(conn)
	enc.Encode(RPCRequest{
		Method: "DeleteKeys",
		Params: mustJSON(struct{ Keys []string }{keyList}),
	})
}

func main() {
	// Define flags
	ip := flag.String("a", "", "IP address to bind and advertise")
	port := flag.Int("p", 0, "Port to bind and listen on")

	joinIP := flag.String("ja", "", "IP of node to join")
	joinPort := flag.Int("jp", 0, "Port of node to join")

	ts := flag.Int("ts", -1, "Time between stabilize() calls (ms)")
	tff := flag.Int("tff", -1, "Time between fix_fingers() calls (ms)")
	tcp := flag.Int("tcp", -1, "Time between check_predecessor() calls (ms)")

	r := flag.Int("r", -1, "Number of successors to maintain")
	manualID := flag.String("i", "", "Optional manual node ID (40 hex chars)")

	// Parse all flags from command line
	flag.Parse()

	// Validate required flags
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

	// Validate join flags
	joining := false
	if *joinIP != "" || *joinPort != 0 {
		if *joinIP == "" || *joinPort == 0 {
			fmt.Println("Error: both --ja and --jp must be specified to join a ring.")
			os.Exit(1)
		}
		joining = true
	}

	// Print summary so we can see it's correct
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

	// Compute our node ID
	var myID *NodeID
	if *manualID != "" {
		id, err := parseHexToID(*manualID)
		if err != nil {
			fmt.Println("Error: invalid manual ID:", err)
			os.Exit(1)
		}
		myID = id
	} else {
		// default: hash "ip:port"
		myID = hashStringToID(fmt.Sprintf("%s:%d", *ip, *port))
	}

	fmt.Println("Computed node ID:", idToHex(myID))

	// Create basic node struct (we'll extend this later)
	selfInfo := NodeInfo{
		ID:   myID,
		IP:   *ip,
		Port: *port,
	}
	node := newChordNode(selfInfo, *r)
	// If joining, contact bootstrap node to find our successor
	if joining {
		// Contact bootstrap node
		bootstrap := NodeInfo{
			ID:   nil, // we don't know its ID yet
			IP:   *joinIP,
			Port: *joinPort,
		}

		succ := rpcFindSuccessor(bootstrap, myID)
		if succ == nil {
			fmt.Println("Join failed: could not find successor")
			os.Exit(1)
		}

		node.Successors[0] = *succ

		fmt.Println("Joined ring. My successor is:", idToHex(succ.ID), succ.IP, succ.Port)
	} else {
		fmt.Println("Created new ring (successor = self)")
	}

	fmt.Println("Node created. Initial state:")
	node.PrintState()

	// Start TCP server in the background
	go func() {
		if err := node.ListenAndServe(); err != nil {
			fmt.Println("Listen error:", err)
			os.Exit(1)
		}
	}()

	go node.stabilizeLoop(time.Duration(*ts) * time.Millisecond)
	go node.fixFingersLoop(time.Duration(*tff) * time.Millisecond)
	go node.checkPredecessorLoop(time.Duration(*tcp) * time.Millisecond)

	// Command loop: read from stdin
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
				// Store locally
				node.Files[keyHex] = FileRecord{Name: name, Content: string(data)}
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
				rec, ok := node.Files[keyHex]
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
