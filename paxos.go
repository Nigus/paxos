/*
 * Paxos
 * CS 3410
 * Ren Quinn
 *
 */

// Hints
// -----
// TODO: Get it to work for a single slot first, then add slots
// Each node has its own map of channels.  When a command is proposed, the proposer puts a channel in its map for its randomly generated command tag.
// It then waits for a message that it can be applied.  This is done on the channel, when the node's apply method is called it sends a value over the channel telling
// itself that it can apply the command it proposed

// Each channel should be buffered the size of the number of nodes

package main

import (
	"bufio"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strings"
	"strconv"
	"time"
)

var verbose bool
var latency int

type APrepare struct {
//Prepare(slot, seq) -> (okay, promised, command)
	Slot Slot
	Seq Sequence
}

type Accept struct {
//Accept(slot, seq, command) -> (okay, promised)
	Slot Slot
	Seq Sequence
	Command Command
}

type PPrepare struct {
	Command Command
}

type PAccept struct {
	N Sequence
	V Command
}

type Decide struct {
	Slot Slot
	Value Command
}

type Request struct {
	APrepare APrepare
	Accept Accept
	PPrepare PPrepare
	PAccept PAccept
	Decide Decide
	Message string
	Address string
}

type Response struct {
	Code int
	Message string
	Okay bool
	Command Command
	Promised Sequence
}

// incremented within a slot
type Sequence struct {
	N int
	Address string
}

func (seq Sequence) String() string {
	return fmt.Sprintf("Sequence{N: %d, Address: %s}", seq.N, seq.Address)
}

func (this Sequence) Cmp(that Sequence) int {
	if (this.N < that.N) {
		return -1
	}
	if (this.N > that.N) {
		return 1
	}
	return 0
}

type Command struct {
	Address string // The Proposer
	Sequence Sequence
	Command string
	Tag int64			// To uniquely identify a command
	Key string
}

func (command Command) String() string {
	return fmt.Sprintf("Command{Tag: %d, Command: %s, Sequence: %s, Address: %s}", command.Tag, command.Command, command.Sequence.String(), command.Address)
}

func (this Command) Eq(that Command) bool {
	return this.Address == that.Address && this.Tag == that.Tag
}

type Slot struct {
	PromiseNumber Sequence // most recently promised sequence number
	Accepted Command // Most recently accepted command
	Decided bool // When it was decided
	N int
}

type Replica struct {
	Address string
	Database map[string] string
	Friends []string
	Promised Sequence
	Slots map[int]Slot
	Acks map[string] chan string
}

func (self *Replica) Ping(_ Request, response *Response) error {
	response.Message = "Successfully Pinged " + self.Address
	log.Println("I Got Pinged")
	return nil
}

/*
 *	ACCEPTOR
 */

//Prepare(slot, seq) -> (okay, promised, command)
//  A prepare request asks the replica to promise not to accept any future
//	prepare or accept messages for slot slot unless they have a higher sequence number than seq.
//
//	If this is the highest seq number it has been asked to promise,
//		it should return True for okay,
//		seq as the new value for promised (which it must also store),
//		and whatever command it most recently accepted (using the Accept operation below).

//	If it has already promised a number >= seq,
//		it should reject the request by returning False for the okay result value.
//		In this case it should also return the highest seq it has promised as the return value promised, and the command return value can be ignored.

/* ==== OLD ====
The response should include the highest-sequenced command that the replica has already accepted (using the accept operation below),
regardless of whether or not it accepts the prepare message and promises n. If it has not accepted any commands in this slot,
it should return an empty/zero value for the command.

If the slot is already decided (and this replica knows it), then it should return an error response.
In addition, it should send out a decide message to the caller (whose address is provided as a).
*/
func (self *Replica) APrepare(req Request, resp *Response) error {
	logM("")
	logM("Received prepare request from " + req.Address)
	// TODO: What do you do with the slot?
	args := req.APrepare
	if self.Promised.Cmp(args.Seq) < 1 {
		resp.Okay = true
		resp.Promised = args.Seq
		resp.Command = self.Slots[self.Promised.N].Accepted
		self.Promised = args.Seq
	} else {
		resp.Okay = false
		resp.Promised = self.Promised
	}
	return nil
}

//Accept(slot, seq, command) -> (okay, promised)
//	An accept request asks the replica to accept the value command for slot slot,
//	but only if the replica has not promised a value greater than seq for this slot
//	(less-than or equal-to is okay, and it is okay if no seq value has ever been promised for this slot).
//
//	If successful,
//		the command should be stored in the slot as the most-recently-accepted value.
//		okay should be True, and promised is the last value promised for this slot.
//
//	If the request fails because a higher value than seq has been promised for this slot,
//		okay should be False and promised should be the last value promised for this slot.

/* ==== OLD ====
The response should include the highest value of n that this replica is aware of for the current slot.
This may be the value of an earlier accepted v value, or it might be the n from a different prepare message.

If the slot is already decided (and this replica knows it), then it should return an error response.
In addition, it should send out a decide message to the caller.
*/
func (self *Replica) AAccept(req Request, resp *Response) error {
	logM("")
	logM("Received accept request from " + req.Address)
	// TODO: What do you do with the slot?
	args := req.Accept

	if (self.Promised.Cmp(args.Seq) < 1) {
		self.Slots[args.Seq.N] = Slot{Accepted: args.Command}
		resp.Okay = true
	} else {
		resp.Okay = false
	}
	// TODO: resp.Promised = the last value promised for this slot
	return nil

	/* ===== OLD ====
	// If the replica has promised 'n' for this slot 's' && promised <= n
	if (self.Slots[args.N] && self.Promised <= args.N) {
		// Ask the replica to accept the value v for slot s
		self.Slots
	} else {
	// else
		// rejected
	}
	//response.n = the highest value of n that this replica is aware of for the current slot

	// if the slot is already decided
		// return an error response
	// response.Message = decide message
	return nil
	*/
}

/*
 *	LEARNER
 */

//Decide(s, v)
//	A decide request indicates that another replica has learned of the decision for this slot.
//	Since we trust other hosts in the cell, we accept the value.
//	It would be good to check if you have already been notified of a decision, and if that decision contradicts this one.
//	In that case, there is an error somewhere and a panic is appropriate.
//
//	If this is the first time that this replica has learned about the decision for this slot,
//	it should also check if it (and possibly slots after it) can now be applied.
func (self *Replica) Decide(req Request, resp *Response) error {
	logM("")
	logM("Received decide message from " + req.Address)
	args := req.Decide
	// TODO: It would be good to check if y ou hav e already been notified of a decision,
	// and if that decision contradicts this one.
	// In that case, there is an error somewhere and a panic is appropriate.
	// TODO: If this is the first time that this replica has learned about the decision for this slot,
	// it should also check if it (and possibly slots after it) can now be applied.

	log.Println("Deciding:", args.Value)
	_, ok := self.Acks[args.Value.Key];
	log.Println("OK:",args.Value.Key)
	log.Println("OK:",self.Acks)
	if ok {
		self.Acks[args.Value.Key] <- args.Value.Command
	}
	// TODO: whenever a decision is applied, check if the command has a channel waiting for it by generating the same address/tag key and looking it up.
		// if found send the result across the channel, then remove the channel from the map and throw it away
		// if it isn't found, do nothing; the command was proposed on a different replica
	return nil

}

/*
 *	PROPOSER
 */

func (self *Replica) PPrepare(req Request, resp *Response) error {
	logM("")
	args := req.PPrepare
	var acceptance PAccept

	round := 1
	n := 1
	rounds: for {

		// Pick a Sequence value N
		// Build the slot
		sl := Slot{N: n}
		// Build the sequence
		se := Sequence{N: n, Address: self.Address}
		args.Command.Sequence = se
		logM("Proposing: " + args.Command.String())
		logM("Proposing: Round:" + strconv.Itoa(round) + ", N:" + strconv.Itoa(n))

		// Send a Prepare message out to the entire cell (using a go routine per replica)
		response := make(chan Response, len(self.Friends))
		for _, v := range self.Friends {
			go func(v string, slot Slot, sequence Sequence, response chan Response) {
				req := Request{Address: self.Address, APrepare: APrepare{Slot: slot, Seq: sequence}}
				var resp Response
				err := call(v, "APrepare", req, &resp)
				if err != nil {
					failure("APrepare (from PPrepare)")
					return
				}
				// Send the response over a channel, we can assume that a majority WILL respond
				response <- resp

			}(v, sl, se, response)
		}

		// Get responses from go routines (in a forever loop)
		numYes := 0
		highestN := 0
		var highestCommand Command
		for numVotes := 0; numVotes < len(self.Friends); numVotes++ {
			// pull from the channel response
			prepareResponse := <-response
			//resp{Command, Promised, Okay}
			if prepareResponse.Okay {
				numYes++

				// make note of the highest n value that any replica returns to you
				// track the highest-sequenced command that has already been accepted by one or more of the replicas
				if prepareResponse.Promised.N > highestN {
					highestN = prepareResponse.Promised.N
					highestCommand = prepareResponse.Command
				}
			}

			// If I have a majority
			if numYes >= majority(len(self.Friends)) {
				break
			}
		}

		// If I have a majority
		if numYes >= majority(len(self.Friends)) {
			// select your value
			acceptance.V = args.Command
			acceptance.N = se

			// TODO: If one or more of those replicas that voted for you have already accepted a value,
			//		you should pick the highest-numbered value from among them, i.e. the one that was accepted with the highest n value.
			if highestCommand.Command != "" {
				fmt.Println(acceptance.V)
				acceptance.V = highestCommand
				fmt.Println(acceptance.V)
			}
			// TODO: If none of the replicas that voted for you included a value, you can pick your own.

			// TODO: In either case, you should associate the value you are about to send out for acceptance with your promised n
			//self.Promised = se

			break rounds
		}

		// 	generate a larger n if necessary for a future round
		n = highestN + 1

		// To pause, pick a random amount of time between, say, 5ms and 10ms. If you fail again, pick a random sleep time between 10ms and 20ms
		duration := float64(5 * round)
		offset := float64(duration) * rand.Float64()
		time.Sleep(time.Millisecond * time.Duration(duration+offset))
		round++
	}

	// TODO: Call the proposer Accept method with the value and the sequence number
	req.PAccept = acceptance
	call(self.Address, "PAccept", req, resp)

	return nil
}

func (self *Replica) PAccept(req Request, resp *Response) error {
	args := req.PAccept
	logM("")
	logM("Received a proposal to accept request from " + req.Address)
	logM("Proposing to accept " + args.V.String())

	sl := Slot{N: args.N.N}
	// Send an accept request to all replicas and gather the results
	response := make(chan Response, len(self.Friends))
	for _, v := range self.Friends {
		go func(v string, slot Slot, sequence Sequence, command Command, response chan Response) {
			req := Request{Address: self.Address, Accept: Accept{Slot: slot, Seq: sequence, Command: command}}
			var resp Response
			err := call(v, "AAccept", req, &resp)
			if err != nil {
				failure("AAccept (from PAccept)")
				return
			}
			// Send the response over a channel, we can assume that a majority WILL respond
			response <- resp

		}(v, sl, args.N, args.V, response)
	}

	// Get responses from go routines (in a forever loop)
	numYes := 0
	highestN := 0
	for numVotes := 0; numVotes < len(self.Friends); numVotes++ {
		// pull from the channel response
		prepareResponse := <-response
		//resp{Command, Promised, Okay}
		if prepareResponse.Okay {
			numYes++

			// make note of the highest n value that any replica returns to you
			// track the highest-sequenced command that has already been accepted by one or more of the replicas
			if prepareResponse.Promised.N > highestN {
				highestN = prepareResponse.Promised.N
			}
		}

		// If I have a majority
		if numYes >= majority(len(self.Friends)) {
			break
		}
	}

	if numYes >= majority(len(self.Friends)) {
		logM("You can now decide.")
		sl2 := Slot{N: args.N.N}
		for _, v := range self.Friends {
			go func(v string, slot Slot, command Command) {
				req := Request{Address: self.Address, Decide: Decide{Slot: slot, Value: command}}
				var resp Response
				err := call(v, "Decide", req, &resp)
				if err != nil {
					failure("Decide (from PAccept)")
					return
				}
			}(v, sl2, args.V)
		}
	} else {
		logM("You can't decide. Start over.")
	}

	return nil
}

func call(address, method string, request Request, response *Response) error {
	client, err := rpc.DialHTTP("tcp", getAddress(address))
	if err != nil {
		log.Println("rpc dial: ", err)
		return err
	}
	defer client.Close()

	err = client.Call("Replica."+method, request, response)
	if err != nil {
		log.Println("rpc call: ", err)
		return err
	}

	return nil
}

func getAddress(v string) string {
	return net.JoinHostPort("localhost", v)
}

func failure(f string) {
	log.Println("Call",f,"has failed.")
}

func help() {
	fmt.Println("==============================================================")
	fmt.Println("                          COMMANDS")
	fmt.Println("==============================================================")
	fmt.Println("help               - Display this message.")
	fmt.Println("dump               - Display info about the current node.")
	fmt.Println("put <key> <value>  - Put a value.")
	fmt.Println("get <key>          - Get a value.")
	fmt.Println("delete <key>       - Delete a value.")
	fmt.Println("quit               - Quit the program.")
	fmt.Println("==============================================================")
}

func majority(size int) int {
	return int(math.Ceil(float64(size)/2))
}

func usage() {
	fmt.Println("Usage: ", os.Args[0], "[-v=<false>], [-l=<n>] <local_port> [<port1>...<portn>] ")
	fmt.Println("     -v        Verbose. Dispay the details of the paxos messages. Default is false")
	fmt.Println("     -l        Latency. Sets the latency between messages as a random duration between [n,2n)")
}

func readLine(readline chan string) {
	// Get command
	reader := bufio.NewReader(os.Stdin)
	line, err := reader.ReadString('\n')
	if err != nil {
		log.Fatal("READLINE ERROR:", err)
	}
	readline <- line
}

func logM(message string) {
	if verbose {
		log.Println(message)
	}
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}

	verbose = true
	latency = 0
	var myport string
	var ports []string
	var next string

	for i, arg := range os.Args {
		if i == 0 {
			continue
		}
		if next == "verbose" {
			if arg == "true" {
				verbose = true
			}
			next = ""
		} else if next == "latency" {
			latency, _ = strconv.Atoi(arg)
			next = ""
		} else if strings.HasPrefix(arg, "-") {
			if arg == "-v" {
				next = "verbose"
			} else if arg == "-l" {
				next = "latency"
			} else {
				fmt.Println("Invalid option: ", arg)
				usage()
				os.Exit(1)
			}
		} else if myport == "" {
			myport = arg
		} else {
			ports = append(ports, arg)
		}
	}

	fmt.Println("-V",verbose)
	fmt.Println("-L",latency)
	fmt.Println("ME",myport)
	fmt.Println("OTHERS",ports)

	// Server Connections
	me := new(Replica)
	me.Address = myport
	me.Friends = append(ports, myport)
	me.Slots = make(map[int]Slot)
	me.Acks = make(map[string]chan string)
	rpc.Register(me)
	rpc.HandleHTTP()

	go func() {
		err := http.ListenAndServe(getAddress(myport), nil)
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}
	}()

	readline := make(chan string, 1)

	mainloop: for {
		go readLine(readline)
		line := <-readline
		l := strings.Split(strings.TrimSpace(line), " ")
		if strings.ToLower(l[0]) == "quit" {
			fmt.Println("QUIT")
			fmt.Println("Goodbye. . .")
			break mainloop
		} else if strings.ToLower(l[0]) == "get" {
			var command Command
			command.Command = strings.Join(l, " ")
			command.Address = me.Address
			// Assign the command a tag
			command.Tag = time.Now().Unix()
			// create a string channel with capacity 1 where the response to the command can be communicated back to the shell code that issued the command
			respChan := make(chan string, 1)
			// store the channel in a map associated with the entire replica. it should map the address and tag number (combined into a string) to the channel
			key := command.Address + strconv.FormatInt(command.Tag, 10)
			command.Key = key
			me.Acks[key] = respChan
			log.Println("Key:", command.Key)

			req := Request{Address: me.Address, PPrepare: PPrepare{Command: command}}
			var resp Response
			err := call(me.Address, "PPrepare", req, &resp)
			if err != nil {
				failure("PPrepare")
				continue
			}

			logM("DONE: " + <-me.Acks[key])
		} else if strings.ToLower(l[0]) == "put" {
			var command Command
			command.Command = strings.Join(l, " ")
			command.Address = me.Address
			// Assign the command a tag
			command.Tag = time.Now().Unix()
			// create a string channel with capacity 1 where the response to the command can be communicated back to the shell code that issued the command
			respChan := make(chan string, 1)
			// store the channel in a map associated with the entire replica. it should map the address and tag number (combined into a string) to the channel
			key := command.Address + strconv.FormatInt(command.Tag, 10)
			command.Key = key
			me.Acks[key] = respChan

			req := Request{Address: me.Address, PPrepare: PPrepare{Command: command}}
			var resp Response
			err := call(me.Address, "PPrepare", req, &resp)
			if err != nil {
				failure("PPrepare")
				continue
			}

			logM("DONE: " + <-me.Acks[key])
		} else if strings.ToLower(l[0]) == "delete" {
			var command Command
			command.Command = strings.Join(l, " ")
			command.Address = me.Address
			// Assign the command a tag
			command.Tag = time.Now().Unix()
			// create a string channel with capacity 1 where the response to the command can be communicated back to the shell code that issued the command
			respChan := make(chan string, 1)
			// store the channel in a map associated with the entire replica. it should map the address and tag number (combined into a string) to the channel
			key := command.Address + strconv.FormatInt(command.Tag, 10)
			command.Key = key
			me.Acks[key] = respChan

			req := Request{Address: me.Address, PPrepare: PPrepare{Command: command}}
			var resp Response
			err := call(me.Address, "PPrepare", req, &resp)
			if err != nil {
				failure("PPrepare")
				continue
			}

			logM("DONE: " + <-me.Acks[key])
		} else if strings.ToLower(l[0]) == "ping" {
			for _, v := range me.Friends {
				address := ":" + v
				fmt.Println("PINGING",address[1:])
				var resp Response
				var req Request
				err := call(address, "Ping", req, &resp)
				if err != nil {
					failure("Ping")
					continue
				}
				log.Println(resp.Message)
			}
		} else {
			help()
		}
	}
}
