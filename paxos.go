/*
 * Paxos distributed system
 *
 */

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
var dumper bool

type Prepare struct {
//Prepare(slot, seq) -> (okay, promised, command)
	Slot Slot
	Seq Sequence
}

type Accepted struct {
//Accepted(slot, seq, command) -> (okay, promised)
	Slot Slot
	Seq Sequence
	Command Command
}

type Propose struct {
	Command Command
}

type Accept struct {
	Slot Slot
	N Sequence
	V Command
}

type Decide struct {
	Slot Slot
	Value Command
}

type Request struct {
	Prepare Prepare
	Accepted Accepted
	Propose Propose
	Accept Accept
	Decide Decide
	Message string
	Address string
}

type Response struct {
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
	if this.N == that.N {
		if this.Address > that.Address {
			return 1
		}
		if this.Address < that.Address {
			return -1
		}
		return 0
	}
	if this.N < that.N {
		return -1
	}
	if this.N > that.N {
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
	Sequence Sequence // most recently promised sequence number
	Command Command // Most recently accepted command
	Decided bool // When it was decided
	N int
}

func (slot Slot) String() string {
	return fmt.Sprintf("Slot{Command: %v, Decided: %t}", slot.Command, slot.Decided)
}

type Replica struct {
	Address string
	Database map[string] string
	Friends []string
	Slots []Slot
	Recent Slot
	Acks map[string] chan string
	Current int
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
func (self *Replica) Prepare(req Request, resp *Response) error {
	// Latency sleep
	duration := float64(latency)
	offset := float64(duration) * rand.Float64()
	time.Sleep(time.Second * time.Duration(duration+offset))
	if dumper {
		var daresp Response
		var dareq Request
		err := call(self.Address, "Dumpall", dareq, &daresp)
		if err != nil {
			failure("DUMPALL")
		}
	}
	////////////////////////////////////////////////////////

	args := req.Prepare
	logM("")
	logM("- Prepare:  Args(SLOT: " + strconv.Itoa(args.Slot.N) + ", SEQUENCE: " + strconv.Itoa(args.Seq.N) + ")")
	logM("- Prepare:  " + args.Slot.String())

	if self.Slots[args.Slot.N].Sequence.Cmp(args.Seq) == -1 {
  //if self.Promised < args.Seq
		logM("- Prepare:  Sequence YES")
		resp.Okay = true
		resp.Promised = args.Seq
		self.Slots[args.Slot.N].Sequence = args.Seq
		resp.Command = self.Slots[args.Slot.N].Command
	} else {
		resp.Okay = false
		logM("- Prepare:  Sequence NO. Already promised a higher number: " + strconv.Itoa(self.Slots[args.Slot.N].Sequence.N))
		resp.Promised = self.Slots[args.Slot.N].Sequence
	}

	return nil
}

//Accepted(slot, seq, command) -> (okay, promised)
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
func (self *Replica) Accepted(req Request, resp *Response) error {
	// Latency sleep
	duration := float64(latency)
	offset := float64(duration) * rand.Float64()
	time.Sleep(time.Second * time.Duration(duration+offset))
	if dumper {
		var daresp Response
		var dareq Request
		err := call(self.Address, "Dumpall", dareq, &daresp)
		if err != nil {
			failure("DUMPALL")
		}
	}
	////////////////////////////////////////////////////////


	args := req.Accepted
	logM("")
	logM("- Accepted:  Args(SLOT: " + strconv.Itoa(args.Slot.N) + ", SEQUENCE: " + strconv.Itoa(args.Seq.N) + ")")
	logM("- Accepted:  " + args.Slot.String())

	if (self.Slots[args.Slot.N].Sequence.Cmp(args.Seq) == 0) {
		logM("- Accepted:  Sequence YES")
		resp.Okay = true
		resp.Promised = self.Slots[args.Slot.N].Sequence
		self.Recent = args.Slot
	} else {
		logM("- Accepted:  Sequence NO. Already promised a higher number: " + strconv.Itoa(self.Slots[args.Slot.N].Sequence.N))
		resp.Okay = false
		//resp.Promised = self.Slots[args.Slot.N].Sequence
		resp.Promised = self.Recent.Sequence
	}
	return nil
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
	// Latency sleep
	duration := float64(latency)
	offset := float64(duration) * rand.Float64()
	time.Sleep(time.Second * time.Duration(duration+offset))
	if dumper {
		var daresp Response
		var dareq Request
		err := call(self.Address, "Dumpall", dareq, &daresp)
		if err != nil {
			failure("DUMPALL")
		}
	}
	////////////////////////////////////////////////////////

	args := req.Decide
	logM("")
	logM("> Decide:  Args(SLOT: " + strconv.Itoa(args.Slot.N) + ", VALUE: " + args.Value.Command + ")")
	logM("> Decide:  " + args.Slot.String())

	if self.Slots[args.Slot.N].Decided && self.Slots[args.Slot.N].Command.Command != args.Value.Command {
		logM("> Decide:  Already decided slot " + strconv.Itoa(args.Slot.N) + " with a different command " + args.Value.Command)
		failure("Decide")
		return nil
	}
	// If already decided, quit
	if self.Slots[args.Slot.N].Decided {
		logM("> Decide:  Already decided slot " + strconv.Itoa(args.Slot.N) + " with command " + args.Value.Command)
		return nil
	}

	_, ok := self.Acks[args.Value.Key];
	if ok {
		// if found send the result across the channel, then remove the channel from the map and throw it away
		self.Acks[args.Value.Key] <- args.Value.Command
	}

	command := strings.Split(args.Value.Command, " ")
	//self.Slots[args.Slot.N] = Slot{Command: args.Value, Decided: true}
	args.Slot.Decided = true
	self.Slots[args.Slot.N] = args.Slot

	// TODO: If this is the first time that this replica has learned about the decision for this slot,
	// it should also check if it (and possibly slots after it) can now be applied.
	fmt.Println(args.Slot.N)
	for !allDecided(self.Slots, args.Slot.N) {
		time.Sleep(time.Second)
	}
	switch(command[0]) {
		case "put":
			log.Println("Put " + command[1] + " " + command[2])
			self.Database[command[1]] = command[2]
			break
		case "get":
			log.Println("Get - Key: " + command[1] + ", Value: " + self.Database[command[1]])
			break
		case "delete":
			log.Println(command[1] + " deleted.")
			delete(self.Database, command[1])
			break
	}
	self.Current++

	return nil
}

/*
 *	PROPOSER
 */

func (self *Replica) Propose(req Request, resp *Response) error {
	// Latency sleep
	lduration := float64(latency)
	loffset := float64(lduration) * rand.Float64()
	time.Sleep(time.Second * time.Duration(lduration+loffset))
	if dumper {
		var daresp Response
		var dareq Request
		err := call(self.Address, "Dumpall", dareq, &daresp)
		if err != nil {
			failure("DUMPALL")
		}
	}
	////////////////////////////////////////////////////////

	args := req.Propose
	logM("")
	logM("* Propose:  Args(COMMAND: " + args.Command.String() + ")")

	var acceptance Accept
	round := 1

	var pSlot Slot
	pSlot.Sequence = args.Command.Sequence
	if pSlot.Sequence.N == 0 {
		pSlot.Sequence.Address = self.Address
		pSlot.Sequence.N = 1
	}
	pSlot.Command = args.Command
	pSlot.Command.Sequence = pSlot.Sequence

	rounds: for {
		// Build the slot for the first undecided slot in Slots
		for i:=1; i<len(self.Slots); i++ {
			if !self.Slots[i].Decided {
				pSlot.N = i
				break
			}
		}
		acceptance.Slot = pSlot

		logM("* Propose:  Round: " + strconv.Itoa(round))
		logM("* Propose:  Slot: " + strconv.Itoa(pSlot.N))
		logM("* Propose:  N: " + strconv.Itoa(pSlot.Sequence.N))


		// Send a Prepare message out to the entire cell (using a go routine per replica)
		response := make(chan Response, len(self.Friends))
		for _, v := range self.Friends {
			go func(v string, slot Slot, sequence Sequence, response chan Response) {
				req := Request{Address: self.Address, Prepare: Prepare{Slot: slot, Seq: sequence}}
				var resp Response
				err := call(v, "Prepare", req, &resp)
				if err != nil {
					failure("Prepare (from Propose)")
					return
				}
				// Send the response over a channel, we can assume that a majority WILL respond
				response <- resp

			}(v, pSlot, pSlot.Sequence, response)
		}

		// Get responses from go routines
		numYes := 0
		numNo := 0
		highestN := 0
		var highestCommand Command
		for numVotes := 0; numVotes < len(self.Friends); numVotes++ {
			// pull from the channel response
			prepareResponse := <-response
			if prepareResponse.Okay {
				numYes++
			} else {
				numNo++
			}

			// make note of the highest n value that any replica returns to you
			if prepareResponse.Promised.N > highestN {
				highestN = prepareResponse.Promised.N
			}
			// track the highest-sequenced command that has already been accepted by one or more of the replicas
			if prepareResponse.Command.Sequence.N > highestCommand.Sequence.N {
				highestCommand = prepareResponse.Command
			}

			// If I have a majority
			if numYes >= majority(len(self.Friends)) || numNo >= majority(len(self.Friends)) {
				break
			}
		}

		// If I have a majority
		if numYes >= majority(len(self.Friends)) {
			// select your value
			// If none of the replicas that voted for you included a value, you can pick your own.
			acceptance.V = pSlot.Command

			// In either case, you should associate the value you are about to send out for acceptance with your promised n
			acceptance.N = pSlot.Sequence

			// If one or more of those replicas that voted for you have already accepted a value, KEY WORD IS ACCEPTED
			// you should pick the highest-numbered value from among them, i.e. the one that was accepted with the highest n value.
			fmt.Println(highestCommand)
			fmt.Println(args.Command)
			if highestCommand.Tag > 0 && highestCommand.Tag != args.Command.Tag {
				acceptance.V = highestCommand
				acceptance.Slot.Command = highestCommand

				req.Accept = acceptance
				call(self.Address, "Accept", req, resp)
			} else {
				break rounds
			}
		}

		// If I don't get a majority
		// Generate a larger n for the next round
		pSlot.Sequence.N = highestN + 1

		// To pause, pick a random amount of time between, say, 5ms and 10ms. If you fail again, pick a random sleep time between 10ms and 20ms
		duration := float64(5 * round)
		offset := float64(duration) * rand.Float64()
		time.Sleep(time.Millisecond * time.Duration(duration+offset))
		round++
	}

	req.Accept = acceptance
	call(self.Address, "Accept", req, resp)

	return nil
}

func (self *Replica) Accept(req Request, resp *Response) error {
	// Latency sleep
	lduration := float64(latency)
	loffset := float64(lduration) * rand.Float64()
	time.Sleep(time.Second * time.Duration(lduration+loffset))
	if dumper {
		var daresp Response
		var dareq Request
		err1 := call(self.Address, "Dumpall", dareq, &daresp)
		if err1 != nil {
			failure("DUMPALL")
		}
	}
	////////////////////////////////////////////////////////


	args := req.Accept
	aSlot := args.Slot
	aN := args.N
	aV := args.V

	logM("")
	logM("* Accept:  Args(COMMAND: " + aV.String() + ", SEQUENCE: " + aN.String() + ", SLOT: " + aSlot.String() + ")")

	// Send an accept request to all replicas and gather the results
	response := make(chan Response, len(self.Friends))
	for _, v := range self.Friends {
		go func(v string, slot Slot, sequence Sequence, command Command, response chan Response) {
			req := Request{Address: self.Address, Accepted: Accepted{Slot: slot, Seq: sequence, Command: command}}
			var resp Response
			err := call(v, "Accepted", req, &resp)
			if err != nil {
				failure("Accepted (from Accept)")
				return
			}
			// Send the response over a channel, we can assume that a majority WILL respond
			response <- resp

		}(v, aSlot, aN, aV, response)
	}

	// Get responses from go routines
	numYes := 0
	numNo := 0
	highestN := 0
	for numVotes := 0; numVotes < len(self.Friends); numVotes++ {
		// pull from the channel response
		prepareResponse := <-response
		//resp{Command, Promised, Okay}
		if prepareResponse.Okay {
			numYes++
		} else {
			numNo++
		}

		// make note of the highest n value that any replica returns to you
		if prepareResponse.Promised.N > highestN {
			highestN = prepareResponse.Promised.N
		}

		// If I have a majority
		if numYes >= majority(len(self.Friends)) || numNo >= majority(len(self.Friends)) {
			break
		}
	}

	if numYes >= majority(len(self.Friends)) {
		logM("* Accept:  Received enough votes, you can now decide.")
		for _, v := range self.Friends {
			go func(v string, slot Slot, command Command) {
				req := Request{Address: self.Address, Decide: Decide{Slot: slot, Value: command}}
				var resp Response
				err := call(v, "Decide", req, &resp)
				if err != nil {
					failure("Decide (from Accept)")
					return
				}
			}(v, aSlot, aV)
		}

		return nil
	}

	logM("* Accept:  Not enough votes.  You can't decide. Start over.")
	aV.Sequence.N = highestN + 1

	// To pause, pick a random amount of time between, say, 5ms and 10ms. If you fail again, pick a random sleep time between 10ms and 20ms
	duration := float64(5)
	offset := float64(duration) * rand.Float64()
	time.Sleep(time.Millisecond * time.Duration(duration+offset))

	req1 := Request{Address: self.Address, Propose: Propose{Command: aV}}
	var resp1 Response
	err := call(self.Address, "Propose", req1, &resp1)
	if err != nil {
		failure("Propose")
		return err
	}

	return nil
}

/*
 * HELPERS
 */

func allDecided(slots []Slot, n int) bool {
	if n == 1 {
		return true
	}
	for k, v := range slots {
		if k == 0 {
			continue
		}
		if k >= n {
			break
		}
		if !v.Decided {
			return false
		}
	}
	return true
}

func (self *Replica) Dumpall(_ Request, _ *Response) error {
	log.Println("====================================")
	log.Println("Database")
	for k, v := range self.Database {
		log.Printf("    Key: %s, Value: %s", k, v)
	}
	log.Println("Current Slot")
	log.Printf("	%d", self.Current)
	log.Println("Slots")
	for k, v := range self.Slots {
		if v.Decided {
			log.Printf("    [%d] %v", k, v)
		}
	}
	log.Println("====================================")
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
	dumper = false
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
			if arg == "false" {
				verbose = false
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

/*
	fmt.Println("-V",verbose)
	fmt.Println("-L",latency)
	fmt.Println("ME",myport)
	fmt.Println("OTHERS",ports)
	*/

	// Server Connections
	me := new(Replica)
	me.Address = myport
	me.Friends = append(ports, myport)
	me.Slots = make([]Slot, 100)
	me.Acks = make(map[string]chan string)
	me.Database = make(map[string]string)
	me.Current = 1
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
		} else if strings.ToLower(l[0]) == "dumpall" {
			for _, v := range me.Friends {
				var resp Response
				var req Request
				err := call(v, "Dumpall", req, &resp)
				if err != nil {
					failure("DUMPALL")
					continue
				}
			}
		} else if strings.ToLower(l[0]) == "get" {
			var command Command
			command.Command = strings.Join(l, " ")
			command.Address = me.Address
			// Assign the command a tag
			command.Tag, _ = strconv.ParseInt(strconv.FormatInt(time.Now().Unix(), 10) + command.Address, 10, 64)
			// create a string channel with capacity 1 where the response to the command can be communicated back to the shell code that issued the command
			respChan := make(chan string, 1)
			// store the channel in a map associated with the entire replica. it should map the address and tag number (combined into a string) to the channel
			key := strconv.FormatInt(command.Tag, 10) + command.Address
			command.Key = key
			me.Acks[key] = respChan

			req := Request{Address: me.Address, Propose: Propose{Command: command}}
			var resp Response
			err := call(me.Address, "Propose", req, &resp)
			if err != nil {
				failure("Propose")
				continue
			}

			go func() {
				logM("DONE: " + <-me.Acks[key])
			}()
		} else if strings.ToLower(l[0]) == "put" {
			var command Command
			command.Command = strings.Join(l, " ")
			command.Address = me.Address
			// Assign the command a tag
			command.Tag, _ = strconv.ParseInt(strconv.FormatInt(time.Now().Unix(), 10) + command.Address, 10, 64)
			// create a string channel with capacity 1 where the response to the command can be communicated back to the shell code that issued the command
			respChan := make(chan string, 1)
			// store the channel in a map associated with the entire replica. it should map the address and tag number (combined into a string) to the channel
			key := strconv.FormatInt(command.Tag, 10) + command.Address
			command.Key = key
			me.Acks[key] = respChan

			req := Request{Address: me.Address, Propose: Propose{Command: command}}
			var resp Response
			err := call(me.Address, "Propose", req, &resp)
			if err != nil {
				failure("Propose")
				continue
			}

			go func() {
				logM("DONE: " + <-me.Acks[key])
			}()
		} else if strings.ToLower(l[0]) == "delete" {
			var command Command
			command.Command = strings.Join(l, " ")
			command.Address = me.Address
			// Assign the command a tag
			command.Tag, _ = strconv.ParseInt(strconv.FormatInt(time.Now().Unix(), 10) + command.Address, 10, 64)
			// create a string channel with capacity 1 where the response to the command can be communicated back to the shell code that issued the command
			respChan := make(chan string, 1)
			// store the channel in a map associated with the entire replica. it should map the address and tag number (combined into a string) to the channel
			key := strconv.FormatInt(command.Tag, 10) + command.Address
			command.Key = key
			me.Acks[key] = respChan

			req := Request{Address: me.Address, Propose: Propose{Command: command}}
			var resp Response
			err := call(me.Address, "Propose", req, &resp)
			if err != nil {
				failure("Propose")
				continue
			}

			go func() {
				logM("DONE: " + <-me.Acks[key])
			}()
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
