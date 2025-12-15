package main

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
)

// --------------- Incomming ----------------

func (n *Node) handleConnection(conn net.Conn) {
	defer conn.Close()

	fmt.Println("Incoming connection from:", conn.RemoteAddr())

	reader := bufio.NewReader(conn)
	line, err := reader.ReadString('\n')
	if err != nil {
		fmt.Println("Error reading from connection:", err)
		return
	}

	msg := strings.TrimSpace(line)
	parts := strings.Fields(msg)
	if len(parts) == 0 {
		fmt.Println("Empty command received")
		return
	}

	cmd := parts[0]

	switch cmd {
	case "FINDSUCC":
		n.rpcFindSucc(conn, parts)
	case "GETPRED":
		n.rpcGetPred(conn)

	case "NOTIFY":
		n.rpcNotify(conn, parts)

	case "CLOSEST":
		n.rpcClosest(conn, parts)

	default:
		fmt.Println("Unknown command:", cmd)
	}

}

// Handlers server side

func (n *Node) rpcFindSucc(conn net.Conn, parts []string) {
	if len(parts) != 2 {
		fmt.Println("Invalid FINDSUCC command")
		return
	}

	id, err := parseHexID(parts[1])
	if err != nil {
		fmt.Println("Invalid ID in FINDSUCC command:", err)
		return
	}

	succ := n.FindSucc(id)

	fmt.Fprintf(conn, "NODE %s %s %d\n", idToHex(succ.ID), succ.IP, succ.Port)
}

func (n *Node) rpcGetPred(conn net.Conn) {
	if n.Predecessor == nil {
		fmt.Fprintf(conn, "NONE\n")
		return
	}

	p := n.Predecessor
	fmt.Fprintf(conn, "NODE %s %s %d\n", idToHex(p.ID), p.IP, p.Port)
}

func (n *Node) rpcNotify(conn net.Conn, parts []string) {
	if len(parts) != 4 {
		fmt.Fprintf(conn, "ERR: Bad NOTIFY\n")
		return
	}

	id, err := parseHexID(parts[1])
	if err != nil {
		fmt.Fprintf(conn, "ERR: Bad ID in NOTIFY\n")
		return
	}

	ip := parts[2]
	port, _ := strconv.Atoi(parts[3])

	candidate := &RemoteNode{
		ID:      id,
		IP:      ip,
		Port:    port,
		Address: fmt.Sprintf("%s:%d", ip, port),
	}

	n.Notify(candidate)
	fmt.Fprintf(conn, "OK, Notification sent\n")
}

func (n *Node) rpcClosest(conn net.Conn, parts []string) {
	if len(parts) != 2 {
		fmt.Fprintf(conn, "ERR Bad CLOSEST\n")
		return
	}

	id, err := parseHexID(parts[1])
	if err != nil {
		fmt.Fprintf(conn, "ERR Bad ID in CLOSEST\n")
		return
	}
	finger := n.ClosestPrecedingFinger(id)

	fmt.Fprintf(conn, "NODE %s %s %d\n", idToHex(finger.ID), finger.IP, finger.Port)
}

// --------------- Outgoing ----------------

func remoteFindsucc(target *RemoteNode, id string) *RemoteNode {
	conn, err := net.Dial("tcp", target.Address)
	if err != nil {
		return nil
	}
	defer conn.Close()

	fmt.Fprintf(conn, "FINDSUCC %s\n", id)
	return readNodeResponse(conn)

}

func remoteGetPredecessor(target *RemoteNode) *RemoteNode {
	conn, err := net.Dial("tcp", target.Address)
	if err != nil {
		return nil
	}
	defer conn.Close()

	fmt.Fprintf(conn, "GETPREDECESSOR\n")

	return readNodeResponse(conn)
}

func remoteClosestPreceedingFinger(target *RemoteNode, id string) *RemoteNode {
	conn, err := net.Dial("tcp", target.Address)
	if err != nil {
		return nil
	}

	defer conn.Close()

	fmt.Fprintf(conn, "CLOSEST %s\n", id)

	return readNodeResponse(conn)
}

// Parse node responses

func readNodeResponse(conn net.Conn) *RemoteNode {
	line, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		return nil
	}

	parts := strings.Fields(strings.TrimSpace(line))
	if len(parts) == 1 && parts[0] == "NONE" {
		return nil
	}

	if len(parts) != 4 || parts[0] != "NODE" {
		return nil
	}

	id, err := parseHexID(parts[1])

	if err != nil {
		return nil
	}

	ip := parts[2]
	port, _ := strconv.Atoi(parts[3])

	return &RemoteNode{
		ID:      id,
		IP:      ip,
		Port:    port,
		Address: fmt.Sprintf("%s:%d", ip, port),
	}
}
