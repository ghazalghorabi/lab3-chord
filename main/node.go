package chord

import (
	"crypto/sha1"
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
	sum := sha1.Sum([]byte(s)) //compute SHA-1 hash (20 bytes)

	id := new(big.Int).SetBytes(sum[:]) // Convert byte to big.Int
	return id
}
