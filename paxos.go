/*
 * Paxos
 * CS 3410
 * Ren Quinn
 *
 */

package main

import (
	"bufio"
	"fmt"
	"log"
	"net/http"
	"net/rpc"
	"os"
	"strings"
	"strconv"
)

type Request struct {
	Message string
}

type Response struct {
	Message string
}

type Replica struct {
	Database map[string] string
}

func (replica *Replica) Ping(_ Request, response *Response) error {
	response.Message = "PINGED"
	log.Println("Got Pinged")
	return nil
}

func call(address string, method string, request Request, response *Response) error {
	client, err := rpc.DialHTTP("tcp", address)
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

func failure(f string) {
	log.Println("Call",f,"has failed.")
}

func help() {
	fmt.Println("==============================================================")
	fmt.Println("                          COMMANDS")
	fmt.Println("==============================================================")
	fmt.Println("help               - Display this message.")
	fmt.Println("dump               - Display info about the current node.")
	fmt.Println("put <key> <value>  - Put a value .")
	fmt.Println("get <key>          - Get a value .")
	fmt.Println("delete <key>       - Delete a value .")
	fmt.Println("quit               - Quit the program.")
	fmt.Println("==============================================================")
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
		log.Fatal("FRL ERROR:", err)
	}
	readline <- line
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}

	verbose := false
	latency := 0
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
	replica := new(Replica)
	rpc.Register(replica)
	rpc.HandleHTTP()

	//serverAddress := net.JoinHostPort("localhost", myport)
	serverAddress := ":" + myport
	go func() {
		err := http.ListenAndServe(serverAddress, nil)
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
		} else if strings.ToLower(l[0]) == "put" {
		} else if strings.ToLower(l[0]) == "delete" {
		} else if strings.ToLower(l[0]) == "ping" {
			address := serverAddress
			if len(l) > 1 && l[1] != "" {
				address = ":" + l[1]
			}
			fmt.Println("PINGING",address[1:])
			var resp Response
			var req Request
			err := call(address, "Ping", req, &resp)
			if err != nil {
				failure("Ping")
				continue
			}
			log.Println(resp.Message)
		} else {
			help()
		}
	}
}
