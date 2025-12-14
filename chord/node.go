package chord

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"math/big"
)

// NodeInfo = has basic information to contact a node in the Chord
const M = 160 // Number of bits in the Chord identifier space (SHA-1 = 160 bits)

var MaxID = new(big.Int).Exp(big.NewInt(2), big.NewInt(M), nil) // Represents the size of the ring (0.. 2^M - 1)

type NodeInfo struct {
	ID   *big.Int // node's identifier in the ring
	IP   string   // IP address as string e.g., 127.0.0.1
	Port int      // port number
}

// Address returns "IP:Port" as a string convinient for net.Listen / net.Dial
func (n NodeInfo) Address() string {
	return fmt.Sprintf("%s:%d", n.IP, n.Port)
}

// HashString takes an arbitarary string and returns its SHA-1 hash as a *big.Int
func HashString(s string) *big.Int {
	sum := sha1.Sum([]byte(s)) //compute SHA-1 hash (20 bytes), this produces a 20 byte array. For example: "Hello.txt" -> [0x9a, 0x2f, 0x67, 0x11, ... 20 bytes total]

	id := new(big.Int).SetBytes(sum[:]) // Convert byte to big.Int. Creates a big.Int that can store large numbers and fills it with 20 bytes. The integer this converts to is the node's ID on the Chord ring
	return id                           // Now the program has a chord identifier
}

func ParseHexToID(hexStr string) (*big.Int, error) { // ParsingHextoID parses a 40 character hex string into a *big.Int

	bytes, err := hex.DecodeString(hexStr) // decode hex string into bytes
	if err != nil {                        // if it does succed return nil otherwise return error
		return nil, err
	}

	id := new(big.Int).SetBytes(bytes) // convert bytes to big Int
	return id, nil
}

// idInInterval checks if the ID lies in the interval (start, end) or (start, end] on the identifier circle modulo 2^M
// this function checks if you start and move forward around the chord ring will it reach end, or do we encounter id along the way
func idInInterval(id, start, end *big.Int, inclusiveEnd bool) bool { // is ID in the interval (start, end] or in (start,end) depending on inclusiveEnd?
	cmpStartend := start.Cmp(end) // compare start and end, telss us what shape the interval has

	//three possible situations for an interval
	//case 1: start < end: it means that the interval looks like this: start ------ end
	//case 2: it means that the interval wraps around the ring: start --------- MAX ---------- 0 ----------- end
	//case 3: start ==end; this is a special case that means the whole ring except the exact point
	if cmpStartend < 0 { // the case where it's the normal interval: start < end: theres no wrap, if start is less than end (so our interval doesnt wrap around the ring)
		//check start < id < end (or <= end if inclusiveEnd)
		if id.Cmp(start) <= 0 { // id is less than or equal to start, reject anything thats <= start, if id <= start, it cnat be inside (start, end) or (start,end]- because the interval requires id > start
			return false // id is not inside the (start, end) or (start, end]
			// if id is before start or equal to start it cant be inside (start,end]
		}
		if inclusiveEnd { // id > start, this is checking the right side of the interval (end)
			return id.Cmp(end) <= 0 // inclusiveEnd == true. If inclusiveEnd is true: id <= end -> interval is (start,end]
			//if inclusiveEnd == true: accept id up to and including end
		}
		return id.Cmp(end) < 0 // if inclusiveEnd == false: Accept id strictly before end: this matches (start, end)

	}

	//case 2: start > end (wrapped interval). This happens when you cross the "0 point" of the ring. So for ex: start = 250 and end = 20 the interval means: (250, 20) SO:(250..255]  U  [0..20]
	// id is inside if id > start OR id <= end (if inclusiveEnd); id >start OR id < end (if not inclusiveEnd)
	if cmpStartend > 0 { // this means the interval crosses 0
		if id.Cmp(start) > 0 { // if id is after start, its definitely inside
			return true
		}
		if inclusiveEnd { // this checks second half of the wrapped interval: 0 -> end (or up to end)
			return id.Cmp(end) <= 0
		}
		return id.Cmp(end) < 0
	}

	//case 3: start == 3nd; if start == end, the interval "around the ring" is basically everything except start itself, because going from start to start loops the entire circle
	return id.Cmp(start) != 0

}

// ChordNode struct + CreateRing and JoinRing without networking
