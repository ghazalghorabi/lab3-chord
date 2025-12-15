package main

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"log"
	"math/big"
	"net"
)

type RemoteNode struct {
	ID      *big.Int
	IP      string
	Port    int
	Address string
}

type Node struct {
	ID      *big.Int
	IP      string
	Port    int
	Address string

	Predecessor   *RemoteNode
	Successor     *RemoteNode
	FingerTable   []*RemoteNode
	SuccessorList []*RemoteNode
	Data          map[string][]byte

	Args Arguments
}

func hashString(s string) *big.Int {
	h := sha1.New()
	h.Write([]byte(s))
	hashBytes := h.Sum(nil)

	hashInt := new(big.Int).SetBytes(hashBytes)
	return hashInt
}

func parseHexID(hexStr string) (*big.Int, error) {
	bytes, err := hex.DecodeString(hexStr)
	if err != nil {
		return nil, err
	}

	id := new(big.Int).SetBytes(bytes)
	return id, nil
}

// Helper to print ID nicely
func idToHex(id *big.Int) string {
	return fmt.Sprintf("%040x", id)
}

func inInterval(x, a, b *big.Int, inclusiveEnd bool) bool {
	mod := new(big.Int).Exp(big.NewInt(2), big.NewInt(160), nil)

	xN := new(big.Int).Mod(x, mod)
	aN := new(big.Int).Mod(a, mod)
	bN := new(big.Int).Mod(b, mod)

	// Normal interval where a<b
	if aN.Cmp(bN) < 0 {
		if inclusiveEnd {
			return xN.Cmp(aN) > 0 && xN.Cmp(bN) <= 0
		}

		return xN.Cmp(aN) > 0 && xN.Cmp(bN) < 0
	}

	//wrap around
	if aN.Cmp(bN) > 0 {
		if inclusiveEnd {
			return xN.Cmp(aN) > 0 || xN.Cmp(bN) <= 0
		}

		return xN.Cmp(aN) > 0 || xN.Cmp(bN) < 0
	}

	return xN.Cmp(aN) != 0
}

func NewNode(args Arguments) *Node {
	n := &Node{
		IP:      args.IP,
		Port:    args.Port,
		Address: fmt.Sprintf("%s:%d", args.IP, args.Port),
		Args:    args,
		Data:    make(map[string][]byte),
	}

	if args.OverrideID != "" {
		id, err := parseHexID(args.OverrideID)
		if err != nil {
			log.Fatal("Invalid OverrideID:", err)
		}
		n.ID = id
	} else {
		n.ID = hashString(n.Address)
	}

	n.FingerTable = make([]*RemoteNode, 160)
	n.SuccessorList = make([]*RemoteNode, 5)

	return n
}

func (n *Node) CreateRing() {
	self := &RemoteNode{
		ID:      n.ID,
		IP:      n.IP,
		Port:    n.Port,
		Address: n.Address,
	}

	n.Successor = self
	n.Predecessor = nil

	fmt.Println("New ring created with node:", n.ID)
	fmt.Println("IP:", n.IP, "Port:", n.Port)

}

func (n *Node) Listen() {
	ln, err := net.Listen("tcp", n.Address)
	if err != nil {
		log.Fatal("Failed to start listener:", err)
	}

	fmt.Println("Chord node listening on", n.Address)

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Println("Failed to accept connection:", err)
			continue
		}
		go n.handleConnection(conn)
	}
}

func (n *Node) JoinRing(joinIP string, joinPort int) {
	target := &RemoteNode{
		IP:      joinIP,
		Port:    joinPort,
		Address: fmt.Sprintf("%s:%d", joinIP, joinPort),
	}

	succ := remoteFindSucc(target, idToHex(n.ID))
	if succ == nil {
		fmt.Println("Join failed: can't contact node ")
		return
	}

	n.Successor = succ
	n.Predecessor = nil

}

func (n *Node) ClosestPrecedingFinger(id *big.Int) *RemoteNode {
	for i := len(n.FingerTable) - 1; i >= 0; i-- {
		f := n.FingerTable[i]
		if f != nil && inInterval(f.ID, n.ID, id, false) {
			return f
		}
	}
	return &RemoteNode{ID: n.ID, IP: n.IP, Port: n.Port, Address: n.Address}
}

func (n *Node) Notify(candidate *RemoteNode) {
	if n.Predecessor == nil {
		n.Predecessor = candidate
		return
	}

	if inInterval(candidate.ID, n.Predecessor.ID, n.ID, false) {
		n.Predecessor = candidate
	}
}

func (n *Node) FindSucc(id *big.Int) *RemoteNode {
	if n.Successor != nil && inInterval(id, n.ID, n.Successor.ID, true) {
		return n.Successor
	}

	cpf := n.ClosestPrecedingFinger(id)

	if cpf.Address == n.Address {
		return n.Successor
	}

	return remoteFindsucc(cpf, idToHex(id))
}
