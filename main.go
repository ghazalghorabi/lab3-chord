package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"
	"time"

	"lab3-chord/chord"
)

func main() {

	//parse CLI args

	a := flag.String("a", "", "IP address this node will bind to and advertise")

	p := flag.Int("p", 0, "Port this node will bind/listen on")

	ja := flag.String("ja", "", "IP address of exsisting chord node to join")

	jp := flag.Int("jp", 0, "Port of exisisting chord node to join")

	ts := flag.Int("ts", 0, "Milliseconds between stabilize invocations")
	tff := flag.Int("tff", 0, "Milliseconds between fix fingers invocations")
	tcp := flag.Int("tcp", 0, "Milliseconds between check predecessor invocations")

	r := flag.Int("r", 0, "Number sucessors to maintain")
	idOverride := flag.String("i", "", "Optional 40-hex-char ID override")

	flag.Parse()

	// validate CLI args

	if *a == "" {
		fatalUsage("missing required -a <ip>")
	}
	if *p <= 0 || *p > 65535 {
		fatalUsage("missing/invalid required -p <port>")
	}

	if *ts < 1 || *ts > 60000 {
		fatalUsage("invalid --ts")
	}

	if *tff < 1 || *tff > 60000 {
		fatalUsage("invalid --tff")
	}

	if *tcp < 1 || *tcp > 60000 {
		fatalUsage("invalid --tcp")
	}

	if *r < 1 || *r > 32 {
		fatalUsage("invalid -r")
	}

	hasJa := *ja != ""
	hasJP := *jp != 0
	if hasJa != hasJP {
		fatalUsage("Join args must be paired: specify both --ja and --jp, or neither")
	}
	if hasJP && (*jp < 1 || *jp > 65535) {
		fatalUsage("Invalid --jp ")
	}

	//valudate optional ID override
	nodeID := chord.HashString(fmt.Sprintf("%s:%d", *a, *p))

	if *idOverride != "" {
		re := regexp.MustCompile(`^[0-9a-fA-F]{40}$`)
		if !re.MatchString(*idOverride) {
			fatalUsage("invalid -i: must be exactly 40 hex chars [0-9a-fA-F]")
		}
		parsed, err := chord.ParseHexToID(*idOverride)
		if err != nil {
			fatalUsage(fmt.Sprintf("invalud -i: %v", err))
		}
		nodeID = parsed

	}

	//create node and start server

	node := chord.NewChordNode(*a, *p, nodeID, *r)

	if err := node.StartServer(); err != nil {
		log.Fatalf("failed to start server on %s:%d: %v", *a, *p, err)
	}

	//create vs join

	if !hasJa {
		node.CreateRing()
		log.Printf("created new ring as %s:%d", *a, *p)
	} else {
		knownAddr := fmt.Sprintf("%s:%d", *ja, *jp)
		knownID := chord.HashString(knownAddr)
		known := chord.NodeInfo{ID: knownID, IP: *ja, Port: *jp}

		node.JoinRingWithKnownSuccessor(known)
		log.Printf("joining ring via %s", knownAddr)
	}

	//periodic scheduling
	startPeriodicRoutines(node,
		time.Duration(*ts)*time.Millisecond,
		time.Duration(*tff)*time.Millisecond,
		time.Duration(*tcp)*time.Millisecond,
	)

	readStdinLoop()

}
func fatalUsage(msg string) {
	fmt.Fprintln(os.Stderr, "error:", msg)
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "example (create new ring:)")
	fmt.Fprintln(os.Stderr, "chord -a 128.8.126.63 -p 4170 --ts 3000 --tff 1000 --tcp 3000 -r 4")
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "example (join exsisting ring):")
	fmt.Fprintln(os.Stderr, "chord -a 128.8.126.63 -p 4171 --ja 128.8.126.63 --jp 4170 --ts 3000 --tff 1000 --tcp 3000 -r 4")
	os.Exit(1)
}

func startPeriodicRoutines(node *chord.ChordNode, ts, tff, tcp time.Duration) {
	stabTicker := time.NewTicker(ts)
	fixTicker := time.NewTicker(tff)
	predTicker := time.NewTicker(tcp)

	go func() {

		defer stabTicker.Stop()
		defer fixTicker.Stop()
		defer predTicker.Stop()

		for {
			select {
			case <-stabTicker.C:
				node.Stabilize()
			case <-fixTicker.C:
				node.FixFingers()
			case <-predTicker.C:
				node.CheckPredecessor()
			}
		}

	}()
}

func readStdinLoop() {
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		fmt.Println("stdin", line)
	}

	select {}
}
