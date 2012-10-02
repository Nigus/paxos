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
	"os"
	"strings"
	"strconv"
)

func Help() {
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
		fmt.Println("Usage: ", os.Args[0], "[-v=<false>], [-l=<n>] <local_port> [<port1>...<portn>] ")
		fmt.Println("     -v        Verbose. Dispay the details of the paxos messages. Default is false")
		fmt.Println("     -l        Latency. Sets the latency between messages as a random duration between [n,2n)")
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
				fmt.Println("Invalide option: ", arg)
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
				fmt.Println("PINGING ME!!!")
		} else {
			Help()
		}
	}
}
