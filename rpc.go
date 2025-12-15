package main

import (
	"fmt"
	"net"
	"strconv"
	"strings"
)

func (n *Node) handleConnection(conn net.Conn) {
	defer conn.Close()

	fmt.Println("Incoming connection from:", conn.RemoteAddr())

	buffer := make([]byte, 1024)
	read, err := conn.Read(buffer)

	if err != nil {
		fmt.Println("Error reading from connection:", err)
		return
	}

	msg := strings.TrimSpace(string(buffer[:read]))
	parts := strings.Split(msg, "")
	cmd := parts[0]

	switch cmd {
	case "FINDSUCCESSOR":
		n.rpcFindSuccessor(conn, parts)
	case "GETPREDECESSOR":
		n.rpcGetPredecessor(conn)

	case "NOTIFY":
		n.rpcNotify(conn, parts)

	case "CLOSEST":
		n.rpcClosest(conn, parts)

	default:
		fmt.Println("Unknown command:", cmd)
	}

}

func (n *Node) rpcFindSuccessor(conn net.Conn, parts []string) {
	if len(parts) != 2 {
		fmt.Println("Invalid FINDSUCCESSOR command")
		return
	}

	id := parts[1]
	successor := n.FindSuccessor(id)

	fmt.Fprintf(conn, "NODE %s %s %d\n", successor.ID, successor.IP, successor.Port)
}

func (n *Node) rpcGetPredecessor(conn net.Conn) {
	if n.Predecessor == nil {
		fmt.Fprintf(conn, "NONE\n")
		return
	}

	p := n.Predecessor
	fmt.Fprintf(conn, "NODE %s %s %d\n", p.ID, p.IP, p.Port)
}

func (n *Node) rpcNotify(conn net.Conn, parts []string) {
	if len(parts) != 4 {
		fmt.Fprintf(conn, "ERR: Bad NOTIFY\n")
		return
	}

	id := parts[1]
	ip := parts[2]
	port, _ := strconv.Atoi(parts[3])

	candidate := &RemoteNode{
		ID:      id,
		IP:      ip,
		Port:    port,
		Address: fmt.Sprintf("%s:%d", ip, port),
	}

	n.Notify(candidate)

	fmt.Fprintf(conn, "OK\n")
}

func (n *Node) rpcClosest(conn net.Conn, parts []string) {
	if len(parts) != 2 {
		fmt.Fprintf(conn, "ERR Bad CLOSEST\n")
		return
	}

	id := parts[1]
	finger := n.ClosestPrecedingFinger(id)

	fmt.Fprintf(conn, "NODE %s %s %d\n", finger.ID, finger.IP, finger.Port)
}

func remoteFindSuccessor(target *RemoteNode, id string) *RemoteNode {
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

func (n *Node) rpcNotify(conn net.conn, parts []string) {
	id := parts[1]
	ip := parts[2]
	port, _ := strconv.Atoi(parts[3])

	candidate := &RemoteNode{
		ID:      id,
		IP:      ip,
		Port:    port,
		Address: fmt.Sprintf("%s:%d", ip, port),
	}

	n.Notify(candidate)

	fmt.Fprintf(conn, "OK\n")
}
