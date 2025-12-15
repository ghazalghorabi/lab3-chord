package main

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"log"
	"net"
)

type RemoteNode struct {
	ID      string
	IP      string
	Port    int
	Address string
}

type Node struct {
	ID      string
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

func computeID(address string) string {
	raw := sha1.Sum([]byte(address))
	return hex.EncodeToString(raw[:])
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
		n.ID = args.OverrideID
	} else {
		n.ID = computeID(n.Address)
	}

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

func (n *Node) handleConnection(conn net.Conn) {
	defer conn.Close()

	fmt.Println("Incoming connection from:", conn.RemoteAddr())

	// Temporary: read whatever they send but ignore it
	buf := make([]byte, 1024)
	conn.Read(buf)

	// Temporary: just send back pong
	conn.Write([]byte("pong"))
}

func (n *Node) JoinRing(joinIP string, joinPort int) {
	addr := fmt.Sprintf("%s:%d", joinIP, joinPort)
	fmt.Println("Trying: joining ring via node at", addr)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		fmt.Println("Connect fialed:", err)
		return
	}

	defer conn.Close()

	conn.Write([]byte("ping"))

	buf := make([]byte, 1024)
	nBytes, _ := conn.Read(buf)

	fmt.Println("Received Reply", string(buf[:nBytes]))

}
