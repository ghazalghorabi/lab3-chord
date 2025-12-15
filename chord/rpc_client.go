package chord

import (
	"encoding/json"
	"fmt"
	"math/big"
	"net"
	"time"
)

// how my node calls other nodes so that stabilization, joining and lookups can work
// this is the one function that does: connect -> send request -> read response
func sendRPC(address string, req RPCRequest, out interface{}) error { // address is the remote node address like: "127.0.0.1:4170", req is what we want to send (Type + Body), out is where we want to store the response data (like a nodeInfoDTO). It returns error if anything fails
	conn, err := net.DialTimeout("tcp", address, 2*time.Second) // Opens a TCP connection to address, uses a timeout of 2 seconds, if the remote node is dead/unreachable, we dont wait forever
	//nodes fail. Timeout prevents the code from freezing

	if err != nil {
		return err // if the connection fails, return the error
	}
	defer conn.Close() // close the connection when the function ends, this prevents leaking open sockets.
	//each rpc is "one connnection per request"

	enc := json.NewEncoder(conn) // enc writes JSON into the TCP connection
	dec := json.NewDecoder(conn) // dec reads JSON from the TCP connection
	//JSON is the rpc protocol

	if err := enc.Encode(req); err != nil { // convert req into JSON and send it to the remote node, if that fails: return the error
		return err // this is sending: “GetSuccessor”, “Notify”, “GetPredecessor”, etc.
	}

	var resp RPCResponse //create a variable to hold the response

	//remote node's server handled your request and replied
	if err := dec.Decode(&resp); err != nil { // read one JSON message back from the remote node and decode it into resp, if the remote node sends garbage or disconnects then return an error
		return err
	}

	if !resp.OK { // if the remote node said the request failed (OK == false), return an error with the message it gave
		return fmt.Errorf("%s", resp.Error) // Example: “unknown rpc type” or “bad JSON”.
	}

	// some RPCs return data (GetSuccessor), some dont (notify)
	if out != nil && resp.Data != nil { // only try to decode response data if the caller provided a place to putt it (out!= nil), response contains data (resp.Data != nil)

		b, _ := json.Marshal(resp.Data) // resp.Data is stored as a genric interface{}, after decoding it often becomes map[string]interface{}, so we need to re-encode resp.Data into JSON bytes (Marshal)
		return json.Unmarshal(b, out)   // decode those bytes into the concrete struct we want (Unmarshal into out)
		//this is how we turn "raw JSON object" into a typed Go struct
	}

	return nil // If everything succeeded, return nil (no error).
}

// this sends: are you alive msg
func RPCing(address string) bool { // a helper that checks if a node is reachable

	req := RPCRequest{Type: "Ping", Body: nil} // build a request whose type is ping, no body is needed

	return sendRPC(address, req, nil) == nil // call SendRPC, if it return no error (==nil), return true, otherwise FALSE
	//used for detecting dead predecessor/successor
}

// tell me your successor
func RPCGetSuccessor(address string) (NodeInfo, error) { // calls a remote node and returns that node's successor as NodeInfo
	req := RPCRequest{Type: "GetSuccessor", Body: nil} //create the request. No body needed

	var dto NodeInfoDTO // make a variable to hold the JSON friendly successor (NodeInfoDto)

	if err := sendRPC(address, req, &dto); err != nil {
		return NodeInfo{}, err // send request to address, decode the response data into dto, if anything fails: return an empty NodeInfo and the error
	}
	return fromDTO(dto) //convert the DTO into real NodeInfo (turn hex string into *big.Int), return it
	//this helps with routing and stabilization
}

// tell me your predecessor
func RPCGetPredecessor(address string) (*NodeInfo, error) { //calls a remote node and returns its predecessot, then returns a pointer becuase predecessor can be "none"(nil)

	req := RPCRequest{Type: "GetPredecessor", Body: nil} //build request

	var dto *NodeInfoDTO // its a pointer because remote might return null (meaning no predecssor), if response data is null, dto will stay nil

	if err := sendRPC(address, req, &dto); err != nil { // call remote node, decode response into dto
		return nil, err
	}

	if dto == nil { // if remote predecessor is null, return nil predecessor and nil error: this means it worked but theres no predecessor
		return nil, nil
	}

	ni, err := fromDTO(*dto) // convert DTO -> real NodeInfo, if conversion fails, return error
	if err != nil {
		return nil, err
	}

	return &ni, nil //return pointer to predecessor NodeInfo
	//this is what stabilize needs: x= successor.predecessor
}

func RPCNotify(address string, me NodeInfo) error { // tell a remote node you should consider me as your predescessor
	bodyBytes, _ := json.Marshal(NotifyBody{Node: toDTO(me)}) //build the notify body: convert me -> NodeInfoDTO. wrap it in NotifyBody{Node: ...}, conert that body struct to JSON bytes
	req := RPCRequest{Type: "Notify", Body: bodyBytes}        //build the request: type: Notify, body contains the json bytes of the notify payload
	return sendRPC(address, req, nil)                         // send the request to the remote node, no return data expected so out is nil. Return error if it fails
	// this is the "notify" step in stabilization, successor.notify(n) in the chord paper
}

func RPCGetSelf(address string) (NodeInfo, error) {
	req := RPCRequest{Type: "GetSelf", Body: nil}
	var dto NodeInfoDTO
	if err := sendRPC(address, req, &dto); err != nil {
		return NodeInfo{}, err
	}

	return fromDTO(dto)
}


func RPCFindSuccessorStep(address string, key *big.Int) (NodeInfo, bool, error){
	body, _ := json.Marshal(FindSuccessorReq{Key: key.Text(16)})
	req := RPCRequest{Type: "FindSuccessorStep", Body: body}


	var resp FindSuccessorResp 
	if err := sendRPC(address, req, &resp); err != nil {
		return NodeInfo{}, false, err
	}

	ni, err _ = fromDTO(resp.Node)
	if err != nil {
		return NodeInfo{}, false, err
	}
	return ni, resp.Done, nil
}


func RPCGetSuccessorList(address string) ([]NodeInfo, error) {
	req := RPCRequest{Type: "GetSuccessorList", Body: nil}
	var dtos []NodeInfoDTO
	if err := sendRPC(address, req, &dtos); err != nil {
		return nil, err
	}

	out := make([]NodeInfo, 0, len(dtos))
	for _, d := range dtos {
		ni, err := fromDTO(d)
		if err != nil {
			return nil, err
		}
		out = append(out, ni)
	}
	return out, nil 
}

func RPCStoreFile(address, name, content string) error {
	body, _ := json.Marshal(StoreFileReq{Name: name, Content: content})
	req := RPCReques{Type: "StoreFile", Body:body}
	return sendRPC(address, req, nil)
}


func RPCGetFile(address, name string) (bool, string, error) {
	body, _ := json.Marshal(GetFileReq{Name: name})
	req := RPCRequest{Type: "GetFile", Body: body}

	var resp GetFileResp
	if err := sendRPC(address, req, &resp); err != nil {
		return false, "", err
	}
	return resp.Found, resp.Content, nil 
}

func RPCListFiles (address string) ([]string, error) {
	req := RPCRequest{Type: "ListFiles", Body:nil}
	var resp StateFilesResp
	if err := sendRPC(address, req, &resp); err != nil {
		return nil, err
	}
	return resp.Files, nil 

}


