package chord

import (
	"encoding/json"
	"fmt"
	"math/big"
)

// how to respond when another chord node connects and sends a request

// a simplified version of NodeInfo for sending over the network
// DTO= data transfer object
type NodeInfoDTO struct { //nodeInfo.ID is a *big.Int, JSON cannt safely send a *big.Int, so we convert ut to a hex string
	IP   string `json:"ip"`
	ID   string `json:"id"`
	Port int    `json:"port"`
} // nodes send each other node information (ID, IP, port) all the time

func toDTO(n NodeInfo) NodeInfoDTO { //converts a real chord node into a JSON safe version
	return NodeInfoDTO{ // create a nre NodeInfoDTO struct
		ID:   n.ID.Text(16), // convert the big integer ID into a hex string, base 16 = hexadecimal. example: big.Int(123456) → "1e240"
		IP:   n.IP,          // copy IP and port directly (they are already JSON free)
		Port: n.Port,
	}

}

func fromDTO(d NodeInfoDTO) (NodeInfo, error) { //converts a JSON node back into a real Chord node
	id, ok := new(big.Int).SetString(d.ID, 16) // take the hex string d.ID and convert it back into a *big.Int
	if !ok {
		return NodeInfo{}, fmt.Errorf("bad id hex") // if the hext string is invalid -> return an
	}
	return NodeInfo{ID: id, IP: d.IP, Port: d.Port}, nil // build a real NodeInfo using the decoded ID, so now we can safely use this node inside the chord logic
}

type NotifyBody struct {
	Node NodeInfoDTO `json:"node"` // when a node says "i might be your predecessor, it sends its node info"
}

// this is the "brain" of networking :D
func (n *ChordNode) handleRPC(req RPCRequest) RPCResponse { //called after a request is recieved over TCP, req.Type tells us what the other node wants

	switch req.Type { //Look at the request type string and decide what action to take
	case "Ping":
		return RPCResponse{OK: true} // reply "yes im alive": used to check if a node has failed

	case "GetSuccessor":
		s := n.Successor()                           // get my current successor (next node clockwise)
		return RPCResponse{OK: true, Data: toDTO(s)} // convert successor to JSON format and send it back
		//Nodes ask this during joins and stabilization
	case "GetPredecessor":
		n.mu.Lock()
		defer n.mu.Unlock() // lock because we are reading a shared state

		if n.Predecessor == nil {
			return RPCResponse{OK: true, Data: nil} // if we dont know our predecessor yet -> return null
		}

		return RPCResponse{OK: true, Data: toDTO(*n.Predecessor)} // otherwise send predeccesoor info

	case "Notify":
		var body NotifyBody //create a struct to hold the request body

		if err := json.Unmarshal(req.Body, &body); err != nil {
			return RPCResponse{OK: false, Error: err.Error()}
		} // decode the JSON body, if malformed -> error

		candidate, err := fromDTO(body.Node) // convert JSON node -> real NodeInfo

		if err != nil {
			return RPCResponse{OK: false, Error: err.Error()} //if conversion failed -> error
		}

		n.Notify(candidate) // run chord logic: is this node a better predecessor?

		return RPCResponse{OK: true} //notify succeeded, this is how nodes learn who comes before them

	default:
		return RPCResponse{OK: false, Error: "unknown rpc type"} // reject unknown requests safely
	}

}
