package transport

import (
	"context"
	"fmt"
	"sync"

	"github.com/prashant/raft-kv/internal/raft"
	"github.com/prashant/raft-kv/internal/transport/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// --- type conversions between raft.* and pb.* ---

func toPBEntries(entries []raft.LogEntry) []*pb.LogEntry {
	out := make([]*pb.LogEntry, len(entries))
	for i, e := range entries {
		out[i] = &pb.LogEntry{Term: e.Term, Index: e.Index, Command: e.Command}
	}
	return out
}

func fromPBEntries(entries []*pb.LogEntry) []raft.LogEntry {
	out := make([]raft.LogEntry, len(entries))
	for i, e := range entries {
		out[i] = raft.LogEntry{Term: e.Term, Index: e.Index, Command: e.Command}
	}
	return out
}

// GRPCTransport implements raft.Transport over gRPC. Connections are dialed
// lazily and cached per peer.
type GRPCTransport struct {
	self  raft.NodeID
	addrs map[raft.NodeID]string

	mu      sync.Mutex
	clients map[raft.NodeID]pb.RaftServiceClient
	conns   map[raft.NodeID]*grpc.ClientConn
	dialer  func(addr string) (*grpc.ClientConn, error)
}

// MaxMessageBytes bounds a single gRPC message. It must be large enough to
// carry a whole InstallSnapshot payload (snapshots are sent in one message).
const MaxMessageBytes = 64 << 20 // 64 MiB

// ServerOptions returns the grpc.ServerOptions the Raft/KV servers should use so
// large snapshots don't exceed the default 4 MiB receive limit.
func ServerOptions() []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.MaxRecvMsgSize(MaxMessageBytes),
		grpc.MaxSendMsgSize(MaxMessageBytes),
	}
}

// NewGRPCTransport builds a transport that dials the given peer addresses.
func NewGRPCTransport(self raft.NodeID, addrs map[raft.NodeID]string) *GRPCTransport {
	return &GRPCTransport{
		self:    self,
		addrs:   addrs,
		clients: make(map[raft.NodeID]pb.RaftServiceClient),
		conns:   make(map[raft.NodeID]*grpc.ClientConn),
		dialer: func(addr string) (*grpc.ClientConn, error) {
			return grpc.NewClient(addr,
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithDefaultCallOptions(
					grpc.MaxCallRecvMsgSize(MaxMessageBytes),
					grpc.MaxCallSendMsgSize(MaxMessageBytes),
				),
			)
		},
	}
}

func (t *GRPCTransport) client(target raft.NodeID) (pb.RaftServiceClient, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if c, ok := t.clients[target]; ok {
		return c, nil
	}
	addr, ok := t.addrs[target]
	if !ok {
		return nil, fmt.Errorf("transport: no address for peer %s", target)
	}
	conn, err := t.dialer(addr)
	if err != nil {
		return nil, err
	}
	c := pb.NewRaftServiceClient(conn)
	t.clients[target] = c
	t.conns[target] = conn
	return c, nil
}

// Close tears down all cached connections.
func (t *GRPCTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, c := range t.conns {
		_ = c.Close()
	}
	return nil
}

func (t *GRPCTransport) RequestVote(ctx context.Context, target raft.NodeID, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	c, err := t.client(target)
	if err != nil {
		return nil, err
	}
	resp, err := c.RequestVote(ctx, &pb.RequestVoteRequest{
		Term:         args.Term,
		CandidateId:  string(args.CandidateID),
		LastLogIndex: args.LastLogIndex,
		LastLogTerm:  args.LastLogTerm,
		PreVote:      args.PreVote,
	})
	if err != nil {
		return nil, err
	}
	return &raft.RequestVoteReply{Term: resp.Term, VoteGranted: resp.VoteGranted}, nil
}

func (t *GRPCTransport) AppendEntries(ctx context.Context, target raft.NodeID, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	c, err := t.client(target)
	if err != nil {
		return nil, err
	}
	resp, err := c.AppendEntries(ctx, &pb.AppendEntriesRequest{
		Term:         args.Term,
		LeaderId:     string(args.LeaderID),
		PrevLogIndex: args.PrevLogIndex,
		PrevLogTerm:  args.PrevLogTerm,
		Entries:      toPBEntries(args.Entries),
		LeaderCommit: args.LeaderCommit,
	})
	if err != nil {
		return nil, err
	}
	return &raft.AppendEntriesReply{
		Term:          resp.Term,
		Success:       resp.Success,
		ConflictTerm:  resp.ConflictTerm,
		ConflictIndex: resp.ConflictIndex,
	}, nil
}

func (t *GRPCTransport) InstallSnapshot(ctx context.Context, target raft.NodeID, args *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	c, err := t.client(target)
	if err != nil {
		return nil, err
	}
	resp, err := c.InstallSnapshot(ctx, &pb.InstallSnapshotRequest{
		Term:              args.Term,
		LeaderId:          string(args.LeaderID),
		LastIncludedIndex: args.LastIncludedIndex,
		LastIncludedTerm:  args.LastIncludedTerm,
		Offset:            args.Offset,
		Data:              args.Data,
		Done:              args.Done,
	})
	if err != nil {
		return nil, err
	}
	return &raft.InstallSnapshotReply{Term: resp.Term}, nil
}

// RaftServer adapts a raft.RPCHandler to the generated gRPC server interface.
type RaftServer struct {
	pb.UnimplementedRaftServiceServer
	h raft.RPCHandler
}

// NewRaftServer wraps a handler (typically a *raft.Node) for gRPC serving.
func NewRaftServer(h raft.RPCHandler) *RaftServer { return &RaftServer{h: h} }

// Register attaches the Raft service to a grpc.Server.
func (s *RaftServer) Register(g *grpc.Server) {
	pb.RegisterRaftServiceServer(g, s)
}

func (s *RaftServer) RequestVote(_ context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	r := s.h.HandleRequestVote(&raft.RequestVoteArgs{
		Term:         req.Term,
		CandidateID:  raft.NodeID(req.CandidateId),
		LastLogIndex: req.LastLogIndex,
		LastLogTerm:  req.LastLogTerm,
		PreVote:      req.PreVote,
	})
	return &pb.RequestVoteResponse{Term: r.Term, VoteGranted: r.VoteGranted}, nil
}

func (s *RaftServer) AppendEntries(_ context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	r := s.h.HandleAppendEntries(&raft.AppendEntriesArgs{
		Term:         req.Term,
		LeaderID:     raft.NodeID(req.LeaderId),
		PrevLogIndex: req.PrevLogIndex,
		PrevLogTerm:  req.PrevLogTerm,
		Entries:      fromPBEntries(req.Entries),
		LeaderCommit: req.LeaderCommit,
	})
	return &pb.AppendEntriesResponse{
		Term:          r.Term,
		Success:       r.Success,
		ConflictTerm:  r.ConflictTerm,
		ConflictIndex: r.ConflictIndex,
	}, nil
}

func (s *RaftServer) InstallSnapshot(_ context.Context, req *pb.InstallSnapshotRequest) (*pb.InstallSnapshotResponse, error) {
	r := s.h.HandleInstallSnapshot(&raft.InstallSnapshotArgs{
		Term:              req.Term,
		LeaderID:          raft.NodeID(req.LeaderId),
		LastIncludedIndex: req.LastIncludedIndex,
		LastIncludedTerm:  req.LastIncludedTerm,
		Offset:            req.Offset,
		Data:              req.Data,
		Done:              req.Done,
	})
	return &pb.InstallSnapshotResponse{Term: r.Term}, nil
}
