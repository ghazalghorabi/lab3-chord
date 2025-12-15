package main

import (
	"flag"
	"log"
)

type Arguments struct {
	IP         string
	Port       int
	JoinIP     string
	JoinPort   int
	TS         int
	TFF        int
	TCP        int
	Successors int
	OverrideID string
}

func parseArgs() Arguments {
	args := Arguments{}

	flag.StringVar(&args.IP, "a", "", "IP address Chord will bind to")
	flag.IntVar(&args.Port, "p", 0, "Port Chord client will bind to")

	flag.StringVar(&args.JoinIP, "ja", "", "IP of machine running a Chord node")
	flag.IntVar(&args.JoinPort, "jp", 0, "Port that an existing Chord node is bound to and listening on")
	flag.IntVar(&args.TS, "ts", 0, "Time in ms between invocations of stabilise")
	flag.IntVar(&args.TFF, "tff", 0, "Time in ms between invocations of fix_fingers")
	flag.IntVar(&args.TCP, "tcp", 0, "Time in ms between invocations of check_predecessor")
	flag.IntVar(&args.Successors, "r", 0, "Number of successors maintianed by each Cord")
	flag.StringVar(&args.OverrideID, "i", "", "ID assigned to Chord ") // Optional argument

	flag.Parse()

	//Validation -------------------------

	if args.IP == "" {
		log.Fatal("Missing argument: -a <IP>")
	}

	if args.Port == 0 {
		log.Fatal("Missing argument: -p <Port>")
	}

	if (args.JoinIP != "" && args.JoinPort == 0) || (args.JoinIP == "" && args.JoinPort != 0) {
		log.Fatal("Both join IP and join Port must be provided to join Chord")
	}

	if args.TS < 1 || args.TS > 60000 {
		log.Fatal("-ts must be in range [1,60000]")
	}

	if args.TFF < 1 || args.TFF > 60000 {
		log.Fatal("-tff must be in range [1,60000]")
	}

	if args.TCP < 1 || args.TCP > 60000 {
		log.Fatal("-tcp must be in range [1,60000]")
	}

	if args.Successors < 1 || args.Successors > 32 {
		log.Fatal("-r must be in range [1,32]")
	}

	if args.OverrideID != "" && len(args.OverrideID) != 40 {
		log.Fatal("-i must be a 40 character string")
	}

	return args
}

func main() {
	args := parseArgs()
	node := NewNode(args)

	if args.JoinIP == "" {
		node.CreateRing()
	} else {
		node.JoinRing(args.JoinIP, args.JoinPort)
	}

	go node.Listen()

	// Keep the program running
	select {}
}
