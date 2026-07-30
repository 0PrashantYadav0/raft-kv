// Command raftkvctl is a thin CLI over the Raft KV client.
//
//	raftkvctl --peers n1=127.0.0.1:9001,n2=127.0.0.1:9002 put foo bar
//	raftkvctl --peers n1=127.0.0.1:9001,n2=127.0.0.1:9002 get foo
//	raftkvctl --peers n1=127.0.0.1:9001,n2=127.0.0.1:9002 del foo
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/prashant/raft-kv/pkg/client"
)

func main() {
	peersFlag := flag.String("peers", "", "comma-separated id=addr list")
	timeout := flag.Duration("timeout", 5*time.Second, "request timeout")
	flag.Usage = usage
	flag.Parse()

	args := flag.Args()
	if *peersFlag == "" || len(args) < 1 {
		usage()
		os.Exit(2)
	}

	addrs, err := parseAddrs(*peersFlag)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(2)
	}

	c, err := client.New(addrs)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	switch strings.ToLower(args[0]) {
	case "put":
		if len(args) != 3 {
			usage()
			os.Exit(2)
		}
		if err := c.Put(ctx, args[1], []byte(args[2])); err != nil {
			fail(err)
		}
		fmt.Println("OK")
	case "get":
		if len(args) != 2 {
			usage()
			os.Exit(2)
		}
		v, found, err := c.Get(ctx, args[1])
		if err != nil {
			fail(err)
		}
		if !found {
			fmt.Println("(nil)")
			os.Exit(1)
		}
		fmt.Println(string(v))
	case "del", "delete":
		if len(args) != 2 {
			usage()
			os.Exit(2)
		}
		if err := c.Delete(ctx, args[1]); err != nil {
			fail(err)
		}
		fmt.Println("OK")
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n", args[0])
		usage()
		os.Exit(2)
	}
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}

func usage() {
	fmt.Fprintln(os.Stderr, `raftkvctl — CLI for a raft-kv cluster

Usage:
  raftkvctl --peers id=addr,... put <key> <value>
  raftkvctl --peers id=addr,... get <key>
  raftkvctl --peers id=addr,... del <key>

Flags:
  --peers     comma-separated id=addr list (required)
  --timeout   per-request timeout (default 5s)

Examples:
  raftkvctl --peers n1=127.0.0.1:9001,n2=127.0.0.1:9002 put foo bar
  raftkvctl --peers n1=127.0.0.1:9001 get foo
  raftkvctl --peers n1=127.0.0.1:9001 del foo`)
}

func parseAddrs(s string) (map[string]string, error) {
	addrs := make(map[string]string)
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		id, addr, ok := strings.Cut(part, "=")
		if !ok {
			return nil, fmt.Errorf("bad peer spec: %s", part)
		}
		addrs[strings.TrimSpace(id)] = strings.TrimSpace(addr)
	}
	if len(addrs) == 0 {
		return nil, fmt.Errorf("no peers")
	}
	return addrs, nil
}
