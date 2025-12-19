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

const mBits = 160
const fingerSize = 32

type NodeID = big.Int

type NodeInfo struct {
	ID   *NodeID
	IP   string
	Port int
}

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

type ChordNode struct {
	mu          sync.Mutex
	Self        NodeInfo
	Predecessor *NodeInfo
	Successors  []NodeInfo
	Fingers     []NodeInfo
	nextFinger  int
	Files       map[string]FileRecord
}

// ======================Hash strings into Chord IDs and convert IDs to/from Hex ================================/

func hashStringToID(s string) *NodeID {
	h := sha1.Sum([]byte(s))
	n := new(big.Int)
	n.SetBytes(h[:])
	return n
}

func idToHex(id *NodeID) string {
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

func parseHexToID(s string) (*NodeID, error) {
	b, err := hex.DecodeString(s)
	if err != nil {
		return nil, err
	}
	n := new(big.Int)
	n.SetBytes(b)
	return n, nil
}

// ============================Node initialization: Build safe starting chord state ====================================================

func newChordNode(self NodeInfo, r int) *ChordNode {
	n := &ChordNode{
		Self:        self,
		Predecessor: nil,
		Successors:  make([]NodeInfo, r),
		Fingers:     make([]NodeInfo, fingerSize),
		nextFinger:  -1,
		Files:       make(map[string]FileRecord),
	}
	// =============================== initialize successor list and finger table with safe defaults ===============================================

	for i := 0; i < r; i++ {
		n.Successors[i] = self
	}
	for i := 0; i < fingerSize; i++ {
		n.Fingers[i] = self
	}

	return n
}

// ================================= Print current node state ====================================
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

	// ============================== RPC methods (each case is one "feature") ================================================================

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

// =============================== Fix one finger entry (update routing shortcut) =============================================================================

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

// =============================== RPC client: ask a node for next-hop toward an ID =============================================================================

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
