package chord

import "encoding/json"

type RPCRequest struct { // this struct represents one request message sent over the network, when node A want to ask node B something for ex: who is your successor? it sends an RPCrequest

	Type string `json:"type"` // the backticks json:"type" tell JSON encoding: "when converting back to JSON call this field type". So in JSON it becomes like: {"type: "GetSuccessor}", ""body"
	// Type tells the receiving node what chord action you want: "ping", "getSuccessor", "getPredecessor", "Notify"
	Body json.RawMessage `json:"body"` // Body json.RawMessage, basically raw JSON bytes, it means dont decode the body yet, keep it as raw JSON until we know what the request type is
}

type RPCResponse struct { // represents the reply to a request, after node B processes node A's request, it responds with an RPCResponse
	OK    bool        `json:"ok"`              // field ok is a boolean (true or false). Json tag makes it appear as "ok", if request succesded:{"ok": true, "data": ...}, this tells the RPC caller if it worked
	Error string      `json:"error,omitempty"` // field error holds an error message as a string, omitempty means: if error is empty "" dont include it in the JSON at all. if something goes wrong (bad JSON, unkown request type etc, here you send the reason)
	Data  interface{} `json:"data,omitempty"`  // field data is the "payload"- the actual result
	//interface{} means this can hold any type, omitempty menas: if its nil/empty; it wont be included in JSON. Ex of what data might contain:For "GetSuccessor": a node (ID, IP, port), For "GetFile": file contents, For "FindSuccessor": the successor node
	//most CHord RPCs return something (like a node information)
}

type FindSuccessorReq struct {
	Key string `json:"key"`
}
type FindSuccessorResp struct {
	Done bool        `json:"done"`
	Node NodeInfoDTO `json:"node"`
}

type StoreFileReq struct {
	Name    string `json:"name"`
	Content string `json:"content"`
}

type GetFileReq struct {
	Name string `json:"name"`
}

type GetFileResp struct {
	Found   bool   `json:"found"`
	Content string `json: "content, omitempty"`
}

type StateFilesResp struct {
	Files []string `json:"files"`
}
