// Package server wires a Raft node, the KV state machine, and the client-facing
// gRPC KVService together.
package server

import (
	"context"
	"errors"

	"github.com/prashant/raft-kv/internal/kv"
	"github.com/prashant/raft-kv/internal/raft"
	"github.com/prashant/raft-kv/internal/transport/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Server implements pb.KVServiceServer on top of a Raft node. Writes go through
// Raft consensus; reads are served from the leader's applied state (returning a
// leader hint for redirection when this node is a follower).
type Server struct {
	pb.UnimplementedKVServiceServer
	node  *raft.Node
	store *kv.Store
}

// New returns a KV server backed by the given node and store.
func New(node *raft.Node, store *kv.Store) *Server {
	return &Server{node: node, store: store}
}

// Register attaches the KV service to a grpc.Server.
func (s *Server) Register(g *grpc.Server) {
	pb.RegisterKVServiceServer(g, s)
}

func (s *Server) leaderHint() string {
	_, _, leader := s.node.State()
	return string(leader)
}

// Put replicates a write through Raft and blocks until it is committed+applied.
func (s *Server) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	cmd, err := kv.Command{Op: kv.OpPut, Key: req.Key, Value: req.Value}.Encode()
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if _, err := s.node.Propose(ctx, cmd); err != nil {
		hint, gerr := classifyProposeErr(err)
		if gerr != nil {
			return nil, gerr
		}
		return &pb.PutResponse{Ok: false, LeaderHint: hint}, nil
	}
	return &pb.PutResponse{Ok: true}, nil
}

// Delete replicates a delete through Raft.
func (s *Server) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	cmd, err := kv.Command{Op: kv.OpDelete, Key: req.Key}.Encode()
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if _, err := s.node.Propose(ctx, cmd); err != nil {
		hint, gerr := classifyProposeErr(err)
		if gerr != nil {
			return nil, gerr
		}
		return &pb.DeleteResponse{Ok: false, LeaderHint: hint}, nil
	}
	return &pb.DeleteResponse{Ok: true}, nil
}

// Get serves a linearizable read. It only succeeds on a leader that holds a
// current-term quorum lease; otherwise it returns a leader hint so the client
// can redirect (and avoids serving stale data from a partitioned old leader).
func (s *Server) Get(_ context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	if !s.node.CanServeRead() {
		return &pb.GetResponse{LeaderHint: s.leaderHint()}, nil
	}
	v, ok := s.store.Get(req.Key)
	return &pb.GetResponse{Found: ok, Value: v}, nil
}

// classifyProposeErr maps a Raft propose error to either a leader hint (when
// this node is not the leader, gerr == nil) or a gRPC status error.
func classifyProposeErr(err error) (hint string, gerr error) {
	var notLeader raft.ErrNotLeader
	if errors.As(err, &notLeader) {
		return string(notLeader.Leader), nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return "", status.Error(codes.DeadlineExceeded, err.Error())
	}
	return "", status.Error(codes.Unavailable, err.Error())
}
