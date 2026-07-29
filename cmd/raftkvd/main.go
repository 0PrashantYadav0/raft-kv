// Command raftkvd runs a single node of the distributed KV store.
//
// Example (3-node cluster on one host):
//
//	raftkvd --id n1 --listen 127.0.0.1:9001 \
//	  --peers n1=127.0.0.1:9001,n2=127.0.0.1:9002,n3=127.0.0.1:9003 \
//	  --data-dir ./data/n1
package main

import (
	"flag"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/prashant/raft-kv/internal/kv"
	"github.com/prashant/raft-kv/internal/raft"
	"github.com/prashant/raft-kv/internal/server"
	"github.com/prashant/raft-kv/internal/storage"
	"github.com/prashant/raft-kv/internal/transport"
	"google.golang.org/grpc"
)

func main() {
	var (
		id        = flag.String("id", "", "unique node id (required)")
		listen    = flag.String("listen", "", "address to listen on, e.g. 127.0.0.1:9001 (required)")
		peersFlag = flag.String("peers", "", "comma-separated id=addr list including self (required)")
		dataDir   = flag.String("data-dir", "", "directory for durable state (required)")
		snapEvery = flag.Uint64("snapshot-threshold", 1000, "take a snapshot every N applied entries (0 disables)")
	)
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	if *id == "" || *listen == "" || *peersFlag == "" || *dataDir == "" {
		logger.Error("--id, --listen, --peers and --data-dir are all required")
		flag.Usage()
		os.Exit(2)
	}

	peers, err := parsePeers(*peersFlag)
	if err != nil {
		logger.Error("invalid --peers", "err", err)
		os.Exit(2)
	}
	if _, ok := peers[raft.NodeID(*id)]; !ok {
		logger.Error("--peers must include this node's id", "id", *id)
		os.Exit(2)
	}

	if err := os.MkdirAll(*dataDir, 0o755); err != nil {
		logger.Error("create data dir", "err", err)
		os.Exit(1)
	}

	store, err := storage.OpenBolt(filepath.Join(*dataDir, "raft.db"))
	if err != nil {
		logger.Error("open storage", "err", err)
		os.Exit(1)
	}

	fsm := kv.NewStore()
	tr := transport.NewGRPCTransport(raft.NodeID(*id), peers)

	peerIDs := make([]raft.NodeID, 0, len(peers))
	for pid := range peers {
		peerIDs = append(peerIDs, pid)
	}

	node, err := raft.NewNode(raft.Config{
		ID:                raft.NodeID(*id),
		Peers:             peerIDs,
		Transport:         tr,
		Storage:           store,
		FSM:               fsm,
		Logger:            logger,
		SnapshotThreshold: *snapEvery,
	})
	if err != nil {
		logger.Error("create node", "err", err)
		os.Exit(1)
	}

	grpcServer := grpc.NewServer(transport.ServerOptions()...)
	transport.NewRaftServer(node).Register(grpcServer)
	server.New(node, fsm).Register(grpcServer)

	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		logger.Error("listen", "err", err)
		os.Exit(1)
	}

	node.Start()
	go func() {
		logger.Info("serving gRPC", "addr", *listen)
		if err := grpcServer.Serve(lis); err != nil {
			logger.Error("grpc serve", "err", err)
		}
	}()

	// Wait for a termination signal, then shut down cleanly.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	logger.Info("shutting down")
	grpcServer.GracefulStop()
	node.Stop()
	_ = tr.Close()
}

func parsePeers(s string) (map[raft.NodeID]string, error) {
	peers := make(map[raft.NodeID]string)
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		id, addr, ok := strings.Cut(part, "=")
		if !ok {
			return nil, errBadPeer(part)
		}
		peers[raft.NodeID(strings.TrimSpace(id))] = strings.TrimSpace(addr)
	}
	if len(peers) == 0 {
		return nil, errBadPeer(s)
	}
	return peers, nil
}

type errBadPeer string

func (e errBadPeer) Error() string { return "bad peer spec: " + string(e) }
