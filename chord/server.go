package chord

import (
	"encoding/json" // lets convert go structs to json text (vice versa)
	"log"
	"net" // provides networking (TCP sockets)
)

func (n *ChordNode) StartServer() error { // defines a method on node n, it starts a TCP server so that other nodes can contact this node. Then it returns error if something goes wrong
	//this is how the node becomes reachable by other nodes (successor queries, notify, file store etc.)

	ln, err := net.Listen("tcp", n.Self.Address()) // net.Listen("tcp", address) opens a TCP "listening socket"
	// n.Self.Address() returns "IP:port" like "127.0.0.1:4170"
	//ln is a listener: it waits for incoming connections
	//err is non-nil if the port is already used or not allowed
	// this is how the node "binds" to -a and -p from the assignment

	if err != nil { // if opening server failed, stop and return the error to the caller, if you cant listen, your node cant join the ring or serve lookups
		return err
	}

	log.Println("Listening on", n.Self.Address())

	go func() { // go starts a new goroutine (a lightweight thread), func(){....} is an anonymous function that runs immediately
		//the accept loop runs in the background while your main thread continues. This means that the node must keep running other stuff (stabilize/fix fingers/ read stdin) while accepting network requests
		for { //infinte loop; the server must keep running forever
			conn, err := ln.Accept() // accept() blocks until someone connects, when a remote node connects, you get:
			// conn: The tcp connection to that node or err: if accept failed
			if err != nil {
				log.Println("Accept error:", err) // if accepting a connection failed, print the error,
				continue                          // skip this iteration and go back to the top of the loop; this prevents your server from crashing due to a temporary error.
				// so if one connection fails the node should keep serving others
			}

			go n.handleConnection(conn) // start a new goroutine to handle this connections, this is important bcs you dont want to block the main accept loop while processing one request
			//many nodes might contact at once. This allows concurrent handling
		}
	}() // end the infinite loop and the goroutine
	return nil // StartServer() returns succesfully, the server is now running in the background. Node is now reachable for RPC calls
}

func (n *ChordNode) handleConnection(conn net.Conn) { // handles exactly one incoming TCP connection, conn represents the wire between the remote node and this node
	defer conn.Close() // when this function finishes, close the connection automatically; this acoids leaving open sockets and leaking resources so each request uses a short connection: request -> response -> close

	//RPC protol: nodes speak JSON messages
	dec := json.NewDecoder(conn) // reads JSON from the connection
	enc := json.NewEncoder(conn) // writes JSON to the connection n

	var req RPCRequest //declare a variable req that will hold the decoded request, RPCRequest is the request message struct (like: {Type:"GetSuccessor", Body:...}))

	if err := dec.Decode(&req); err != nil { // try to read one JSON message from the connection and decode it into req, if decoding fails (bad JSON/Connection closed), go into the error block
		enc.Encode(RPCResponse{OK: false, Error: err.Error()}) // send back a json response indicating failure: OK: false, error: <reason>. Then stop handling this connection
		return                                                 // if a node sneds garbage or a broken message, we fail instad of crashing
	}

	resp := n.handleRPC(req) // calls the RPC dispatcher, it looks at the req.Type and decides what to do. example if type is: getSuccersor return n.Succesor[0] if type is notify then update the predecessor
	//this is the bridge between network request arrives and chord algorithm runs. This is where the chord protocol is actually executed
	enc.Encode(resp) // send the response back as JSON, then the function ends and defer conn.Close() closes tthe connection

	// so the code in this file opens a TCP socket at self.IP:self.Port and waits for connections
	// for each connection, reads 1 JSON request and writes one JSON response
	//this is the foundation for all distributed chord behaviour
}
