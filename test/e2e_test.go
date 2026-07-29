package e2e_test

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/prashant/raft-kv/internal/kv"
	"github.com/prashant/raft-kv/internal/raft"
	"github.com/prashant/raft-kv/internal/server"
	"github.com/prashant/raft-kv/internal/storage"
	"github.com/prashant/raft-kv/internal/transport"
	"github.com/prashant/raft-kv/pkg/client"
	"google.golang.org/grpc"
)

type node struct {
	id   raft.NodeID
	raft *raft.Node
	grpc *grpc.Server
	lis  net.Listener
	tr   *transport.GRPCTransport
}

type cluster struct {
	nodes map[raft.NodeID]*node
	addrs map[string]string
}

func startCluster(t *testing.T, n int) *cluster {
	t.Helper()

	// Reserve listeners first so we know every address up front.
	listeners := make(map[raft.NodeID]net.Listener)
	peers := make(map[raft.NodeID]string)
	for i := 0; i < n; i++ {
		id := raft.NodeID(fmt.Sprintf("n%d", i+1))
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		listeners[id] = lis
		peers[id] = lis.Addr().String()
	}

	peerIDs := make([]raft.NodeID, 0, n)
	for id := range peers {
		peerIDs = append(peerIDs, id)
	}

	c := &cluster{nodes: make(map[raft.NodeID]*node), addrs: make(map[string]string)}
	for id, lis := range listeners {
		tr := transport.NewGRPCTransport(id, peers)
		stg, err := storage.OpenBolt(filepath.Join(t.TempDir(), string(id)+".db"))
		if err != nil {
			t.Fatalf("storage: %v", err)
		}
		fsm := kv.NewStore()
		rn, err := raft.NewNode(raft.Config{
			ID:                 id,
			Peers:              peerIDs,
			Transport:          tr,
			Storage:            stg,
			FSM:                fsm,
			HeartbeatInterval:  30 * time.Millisecond,
			ElectionTimeoutMin: 150 * time.Millisecond,
			ElectionTimeoutMax: 300 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("node: %v", err)
		}
		g := grpc.NewServer(transport.ServerOptions()...)
		transport.NewRaftServer(rn).Register(g)
		server.New(rn, fsm).Register(g)

		rn.Start()
		go g.Serve(lis)

		c.nodes[id] = &node{id: id, raft: rn, grpc: g, lis: lis, tr: tr}
		c.addrs[string(id)] = peers[id]
	}
	return c
}

func (c *cluster) stop() {
	for _, n := range c.nodes {
		n.grpc.Stop()
		n.raft.Stop()
		_ = n.tr.Close()
	}
}

func (c *cluster) kill(id raft.NodeID) {
	n := c.nodes[id]
	n.grpc.Stop()
	n.raft.Stop()
	_ = n.tr.Close()
	delete(c.nodes, id)
}

func (c *cluster) leader(timeout time.Duration) raft.NodeID {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for id, n := range c.nodes {
			if n.raft.IsLeader() {
				return id
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	return ""
}

func TestEndToEndFailover(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e in -short mode")
	}
	c := startCluster(t, 3)
	defer c.stop()

	if c.leader(3*time.Second) == "" {
		t.Fatal("no leader elected")
	}

	cli, err := client.New(c.addrs)
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	ctx := context.Background()
	// Basic write/read round trip through the real gRPC stack.
	if err := cli.Put(ctx, "hello", []byte("world")); err != nil {
		t.Fatalf("put: %v", err)
	}
	v, found, err := cli.Get(ctx, "hello")
	if err != nil || !found || string(v) != "world" {
		t.Fatalf("get hello = %q found=%v err=%v", v, found, err)
	}

	// Kill the leader; the client must transparently fail over.
	leader := c.leader(2 * time.Second)
	c.kill(leader)
	if c.leader(5*time.Second) == "" {
		t.Fatal("no new leader after failover")
	}

	// Old data survives and new writes go through the new leader.
	v, found, err = cli.Get(ctx, "hello")
	if err != nil || !found || string(v) != "world" {
		t.Fatalf("post-failover get hello = %q found=%v err=%v", v, found, err)
	}
	if err := cli.Put(ctx, "after", []byte("failover")); err != nil {
		t.Fatalf("post-failover put: %v", err)
	}
	v, found, err = cli.Get(ctx, "after")
	if err != nil || !found || string(v) != "failover" {
		t.Fatalf("get after = %q found=%v err=%v", v, found, err)
	}
}
