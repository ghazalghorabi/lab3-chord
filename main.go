package main

import (
	"bufio"
	"crypto/sha1"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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

type FileRecord struct {
	Name    string
	Content string
}

// ChordNode holds all state for our node (we'll fill it later)
type ChordNode struct {
	Self        NodeInfo
	Predecessor *NodeInfo
	Successors  []NodeInfo
	Fingers     []NodeInfo

	Files map[string]FileRecord // key = hex-encoded ID
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

	reader := bufio.NewReader(conn)
	line, err := reader.ReadString('\n')
	if err != nil {
		return
	}
	line = strings.TrimSpace(line)

	parts := strings.Fields(line)
	cmd := parts[0]

	switch cmd {
	case "PING":
		fmt.Fprintln(conn, "PONG")

	case "GETSUCC":
		// Return our first successor, not ourselves
		succ := n.Successors[0]
		fmt.Fprintf(conn, "%s %s %d\n", idToHex(succ.ID), succ.IP, succ.Port)

	case "GETPRED":
		if n.Predecessor == nil {
			fmt.Fprintln(conn, "NIL")
		} else {
			p := n.Predecessor
			fmt.Fprintf(conn, "%s %s %d\n", idToHex(p.ID), p.IP, p.Port)
		}
	case "GETFILE":
		// GETFILE <key>
		if len(parts) != 2 {
			fmt.Fprintln(conn, "ERR")
			return
		}

		key := parts[1]
		rec, ok := n.Files[key]
		if !ok {
			fmt.Fprintln(conn, "NIL")
			return
		}

		fmt.Fprintf(conn, "OK %s %d\n", rec.Name, len(rec.Content))
		conn.Write([]byte(rec.Content))

	case "NOTIFY":
		// NOTIFY <id> <ip> <port>
		if len(parts) != 4 {
			fmt.Fprintln(conn, "ERR")
			return
		}
		newID, err := parseHexToID(parts[1])
		if err != nil {
			fmt.Fprintln(conn, "ERR")
			return
		}
		portVal, _ := strconv.Atoi(parts[3])
		newPred := &NodeInfo{ID: newID, IP: parts[2], Port: portVal}

		if n.Predecessor == nil || inInterval(newPred.ID, n.Predecessor.ID, n.Self.ID, false) {
			n.Predecessor = newPred
		}
		fmt.Fprintln(conn, "OK")

	case "CPF":
		// CPF <id>
		if len(parts) != 2 {
			fmt.Fprintln(conn, "ERR")
			return
		}
		target, _ := parseHexToID(parts[1])
		finger := n.closestPrecedingFinger(target)
		fmt.Fprintf(conn, "%s %s %d\n", idToHex(finger.ID), finger.IP, finger.Port)

	case "FINDSUCC":
		if len(parts) != 2 {
			fmt.Fprintln(conn, "ERR")
			return
		}
		target, _ := parseHexToID(parts[1])
		succ := n.findSuccessor(target)
		if succ == nil {
			fmt.Fprintln(conn, "ERR")
			return
		}
		fmt.Fprintf(conn, "%s %s %d\n", idToHex(succ.ID), succ.IP, succ.Port)
	case "PUTFILE":
		// PUTFILE <key> <name> <size>
		if len(parts) != 4 {
			fmt.Fprintln(conn, "ERR")
			return
		}

		keyHex := parts[1]
		name := parts[2]
		size, _ := strconv.Atoi(parts[3])

		buf := make([]byte, size)
		_, err := io.ReadFull(reader, buf)
		if err != nil {
			fmt.Fprintln(conn, "ERR")
			return
		}

		n.Files[keyHex] = FileRecord{
			Name:    name,
			Content: string(buf),
		}

		fmt.Fprintln(conn, "OK")
	case "GETSUCCLIST":
		// Send our successor list length, then each successor on its own line
		// First line: number of successors we will send
		count := len(n.Successors)
		fmt.Fprintln(conn, count)
		for _, s := range n.Successors {
			fmt.Fprintf(conn, "%s %s %d\n", idToHex(s.ID), s.IP, s.Port)
		}
	case "GETRANGE":
		// GETRANGE <lowHex> <highHex>
		if len(parts) != 3 {
			fmt.Fprintln(conn, "ERR")
			return
		}

		lowHex := parts[1]
		highHex := parts[2]

		lowID, err1 := parseHexToID(lowHex)
		highID, err2 := parseHexToID(highHex)
		if err1 != nil || err2 != nil {
			fmt.Fprintln(conn, "ERR")
			return
		}

		// Collect matching keys
		sendList := make([]FileRecord, 0)
		sendKeys := make([]string, 0)

		for keyHex, rec := range n.Files {
			keyID, err := parseHexToID(keyHex)
			if err != nil {
				continue
			}

			if inInterval(keyID, lowID, highID, true) {
				sendList = append(sendList, rec)
				sendKeys = append(sendKeys, keyHex)
			}
		}

		// Send count
		fmt.Fprintf(conn, "COUNT %d\n", len(sendList))

		// Send each file (header + content)
		for i := range sendList {
			rec := sendList[i]
			key := sendKeys[i]
			fmt.Fprintf(conn, "%s %s %d\n", key, rec.Name, len(rec.Content))
			conn.Write([]byte(rec.Content))
		}

		// Delete migrated keys
		for _, k := range sendKeys {
			delete(n.Files, k)
		}
		return

	default:
		fmt.Fprintln(conn, "ERR unknown command")
	}
}

// ListenAndServe starts a TCP server for this node and handles requests forever.
func (n *ChordNode) ListenAndServe() error {
	addr := fmt.Sprintf("%s:%d", n.Self.IP, n.Self.Port)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	fmt.Println("Listening on", addr)

	for {
		conn, err := ln.Accept()
		if err != nil {
			// transient error: just keep going
			continue
		}
		go n.handleConnection(conn)
	}
}

func (n *ChordNode) stabilize() {
	// First check: is successor alive?
	if err := tryPing(n.Successors[0]); err != nil {
		if len(n.Successors) > 1 {
			n.Successors[0] = n.Successors[1]
		}
		return // stop stabilize early
	}

	// Now safe to query successor
	x := rpcGetPredecessor(n.Successors[0])

	if x != nil && inInterval(x.ID, n.Self.ID, n.Successors[0].ID, false) {
		n.Successors[0] = *x
	}

	// After updating Successors[0]
	other := rpcGetSuccessorList(n.Successors[0])
	if other != nil {
		for i := 1; i < len(n.Successors); i++ {
			if i < len(other) {
				n.Successors[i] = other[i-1]
			}
		}
	}

	// Notify successor
	addr := fmt.Sprintf("%s:%d", n.Successors[0].IP, n.Successors[0].Port)
	conn, err := net.Dial("tcp", addr)
	if err == nil {
		fmt.Fprintf(conn, "NOTIFY %s %s %d\n",
			idToHex(n.Self.ID), n.Self.IP, n.Self.Port)
		conn.Close()
	}
}

func (n *ChordNode) stabilizeLoop(interval time.Duration) {
	for {
		time.Sleep(interval)
		n.stabilize()
		fmt.Println("[stabilize] done")
	}
}

func (n *ChordNode) checkPredecessor() {
	if n.Predecessor == nil {
		return
	}

	addr := fmt.Sprintf("%s:%d", n.Predecessor.IP, n.Predecessor.Port)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		// predecessor failed
		n.Predecessor = nil
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

var nextFinger = 0

func (n *ChordNode) fixFingers() {
	nextFinger = (nextFinger + 1) % mBits

	start := new(big.Int)
	twoPow := new(big.Int).Exp(big.NewInt(2), big.NewInt(int64(nextFinger)), nil)
	start.Add(n.Self.ID, twoPow)
	start.Mod(start, new(big.Int).Exp(big.NewInt(2), big.NewInt(160), nil))

	succ := n.findSuccessor(start)
	if succ != nil {
		n.Fingers[nextFinger] = *succ
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
	// Start at ourselves
	cur := n.Self

	for {
		// Ask the current node who its successor is
		succ := rpcGetSuccessor(cur)
		if succ == nil {
			return nil
		}

		// If the ID is between cur and succ → answer found
		if inInterval(id, cur.ID, succ.ID, true) {
			return succ
		}

		// Ask THAT node (not us!) for its closest preceding finger
		cpf := rpcClosestPrecedingFinger(cur, id)
		if cpf == nil {
			return succ
		}

		// If no progress can be made, give up and return successor
		if cpf.ID.Cmp(cur.ID) == 0 {
			return succ
		}

		// Hop to next node
		cur = *cpf
	}
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
func (n *ChordNode) closestPrecedingFinger(target *NodeID) NodeInfo {
	// We scan backwards from the largest finger index.
	for i := len(n.Fingers) - 1; i >= 0; i-- {
		finger := n.Fingers[i]
		if finger.ID != nil && inInterval(finger.ID, n.Self.ID, target, false) {
			return finger
		}
	}
	// If none match, return ourselves.
	return n.Self
}

func rpcGetPredecessor(target NodeInfo) *NodeInfo {
	addr := fmt.Sprintf("%s:%d", target.IP, target.Port)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil
	}
	defer conn.Close()

	fmt.Fprintln(conn, "GETPRED")

	resp, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		return nil
	}
	resp = strings.TrimSpace(resp)

	if resp == "NIL" {
		return nil
	}

	parts := strings.Fields(resp)
	if len(parts) != 3 {
		return nil
	}

	id, err := parseHexToID(parts[0])
	if err != nil {
		return nil
	}
	portVal, err := strconv.Atoi(parts[2])
	if err != nil {
		return nil
	}

	return &NodeInfo{
		ID:   id,
		IP:   parts[1],
		Port: portVal,
	}
}

func rpcClosestPrecedingFinger(target NodeInfo, id *NodeID) *NodeInfo {
	addr := fmt.Sprintf("%s:%d", target.IP, target.Port)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil
	}
	defer conn.Close()

	fmt.Fprintf(conn, "CPF %s\n", idToHex(id))

	line, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		return nil
	}

	parts := strings.Fields(strings.TrimSpace(line))
	if len(parts) != 3 {
		return nil
	}

	nid, _ := parseHexToID(parts[0])
	portVal, _ := strconv.Atoi(parts[2])

	return &NodeInfo{
		ID:   nid,
		IP:   parts[1],
		Port: portVal,
	}
}

func rpcGetSuccessor(target NodeInfo) *NodeInfo {
	addr := fmt.Sprintf("%s:%d", target.IP, target.Port)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil
	}
	defer conn.Close()

	fmt.Fprintln(conn, "GETSUCC")

	resp, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		return nil
	}
	parts := strings.Fields(resp)
	if len(parts) != 3 {
		return nil
	}

	id, err := parseHexToID(parts[0])
	if err != nil {
		return nil
	}
	portVal, err := strconv.Atoi(parts[2])
	if err != nil {
		return nil
	}

	return &NodeInfo{
		ID:   id,
		IP:   parts[1],
		Port: portVal,
	}
}

func rpcFindSuccessor(target NodeInfo, id *NodeID) *NodeInfo {
	addr := fmt.Sprintf("%s:%d", target.IP, target.Port)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil
	}
	defer conn.Close()

	fmt.Fprintf(conn, "FINDSUCC %s\n", idToHex(id))

	resp, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		return nil
	}
	resp = strings.TrimSpace(resp)
	parts := strings.Fields(resp)
	if len(parts) != 3 {
		return nil
	}

	newID, err := parseHexToID(parts[0])
	if err != nil {
		return nil
	}
	portVal, err := strconv.Atoi(parts[2])
	if err != nil {
		return nil
	}

	return &NodeInfo{
		ID:   newID,
		IP:   parts[1],
		Port: portVal,
	}
}
func rpcPutFile(target NodeInfo, key, name, content string) bool {
	addr := fmt.Sprintf("%s:%d", target.IP, target.Port)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return false
	}
	defer conn.Close()

	fmt.Fprintf(conn, "PUTFILE %s %s %d\n", key, name, len(content))
	conn.Write([]byte(content))

	resp, _ := bufio.NewReader(conn).ReadString('\n')
	return strings.TrimSpace(resp) == "OK"
}
func rpcGetFile(target NodeInfo, key string) *FileRecord {
	addr := fmt.Sprintf("%s:%d", target.IP, target.Port)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil
	}
	defer conn.Close()

	fmt.Fprintf(conn, "GETFILE %s\n", key)

	reader := bufio.NewReader(conn)
	header, err := reader.ReadString('\n')
	if err != nil {
		return nil
	}

	header = strings.TrimSpace(header)
	if header == "NIL" {
		return nil
	}

	parts := strings.Fields(header)
	if len(parts) != 3 || parts[0] != "OK" {
		return nil
	}

	name := parts[1]
	size, _ := strconv.Atoi(parts[2])

	buf := make([]byte, size)
	_, err = io.ReadFull(reader, buf)
	if err != nil {
		return nil
	}

	return &FileRecord{
		Name:    name,
		Content: string(buf),
	}
}

func rpcGetRange(target NodeInfo, lowHex, highHex string) map[string]FileRecord {
	addr := fmt.Sprintf("%s:%d", target.IP, target.Port)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil
	}
	defer conn.Close()

	fmt.Fprintf(conn, "GETRANGE %s %s\n", lowHex, highHex)

	reader := bufio.NewReader(conn)

	// First line must be: COUNT <n>
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil
	}
	parts := strings.Fields(strings.TrimSpace(line))
	if len(parts) != 2 || parts[0] != "COUNT" {
		return nil
	}

	count, _ := strconv.Atoi(parts[1])
	result := make(map[string]FileRecord)

	for i := 0; i < count; i++ {
		header, err := reader.ReadString('\n')
		if err != nil {
			break
		}
		h := strings.Fields(strings.TrimSpace(header))
		if len(h) != 3 {
			break
		}

		key := h[0]
		name := h[1]
		size, _ := strconv.Atoi(h[2])

		buf := make([]byte, size)
		_, err = io.ReadFull(reader, buf)
		if err != nil {
			break
		}

		result[key] = FileRecord{
			Name:    name,
			Content: string(buf),
		}
	}

	return result
}

func rpcGetSuccessorList(target NodeInfo) []NodeInfo {
	addr := fmt.Sprintf("%s:%d", target.IP, target.Port)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil
	}
	defer conn.Close()

	// Ask for successor list
	fmt.Fprintln(conn, "GETSUCCLIST")

	reader := bufio.NewReader(conn)

	// First line: how many successors
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil
	}
	line = strings.TrimSpace(line)
	count, err := strconv.Atoi(line)
	if err != nil || count <= 0 {
		return nil
	}

	result := make([]NodeInfo, 0, count)
	for i := 0; i < count; i++ {
		row, err := reader.ReadString('\n')
		if err != nil {
			return result // return what we got so far
		}
		row = strings.TrimSpace(row)
		parts := strings.Fields(row)
		if len(parts) != 3 {
			continue
		}

		id, err := parseHexToID(parts[0])
		if err != nil {
			continue
		}
		portVal, err := strconv.Atoi(parts[2])
		if err != nil {
			continue
		}

		result = append(result, NodeInfo{
			ID:   id,
			IP:   parts[1],
			Port: portVal,
		})
	}

	return result
}

func tryPing(n NodeInfo) error {
	addr := fmt.Sprintf("%s:%d", n.IP, n.Port)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	fmt.Fprintln(conn, "PING")
	return nil
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
		// Attempt immediate key migration from successor
		succLow := node.Predecessor
		if succLow == nil {
			// if we don't know predecessor yet, assume full interval from successor->self
			succLow = &NodeInfo{ID: succ.ID}
		}

		lowHex := idToHex(succLow.ID)
		highHex := idToHex(myID)

		files := rpcGetRange(*succ, lowHex, highHex)
		if files != nil {
			for k, rec := range files {
				node.Files[k] = rec
			}
		}

		fmt.Println("Migrated", len(files), "keys from successor.")

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
			for i := 1; i < len(node.Successors); i++ {
				succ := node.Successors[i]
				rpcPutFile(succ, keyHex, name, string(data))
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
