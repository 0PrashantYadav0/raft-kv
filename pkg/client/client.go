// Package client is a Go client for the Raft KV store. It transparently finds
// the leader and retries on leader changes and transient unavailability, so
// callers survive rolling node failures.
package client

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/prashant/raft-kv/internal/transport/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// maxMessageBytes bounds a single gRPC message on the client side.
const maxMessageBytes = 64 << 20 // 64 MiB

// Client talks to a cluster of KV nodes.
type Client struct {
	mu      sync.Mutex
	addrs   map[string]string // nodeID -> address
	conns   map[string]pb.KVServiceClient
	rawConn map[string]*grpc.ClientConn
	leader  string // best guess of current leader nodeID
	order   []string

	MaxRetries int
}

// New builds a client for the given nodeID->address map.
func New(addrs map[string]string) (*Client, error) {
	if len(addrs) == 0 {
		return nil, fmt.Errorf("client: no addresses provided")
	}
	c := &Client{
		addrs:      addrs,
		conns:      make(map[string]pb.KVServiceClient),
		rawConn:    make(map[string]*grpc.ClientConn),
		MaxRetries: 10,
	}
	for id := range addrs {
		c.order = append(c.order, id)
	}
	return c, nil
}

func (c *Client) conn(id string) (pb.KVServiceClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if cl, ok := c.conns[id]; ok {
		return cl, nil
	}
	addr, ok := c.addrs[id]
	if !ok {
		return nil, fmt.Errorf("client: unknown node %s", id)
	}
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(maxMessageBytes),
			grpc.MaxCallSendMsgSize(maxMessageBytes),
		),
	)
	if err != nil {
		return nil, err
	}
	cl := pb.NewKVServiceClient(conn)
	c.conns[id] = cl
	c.rawConn[id] = conn
	return cl, nil
}

// candidateOrder returns the node to try first (last known leader) followed by
// the rest.
func (c *Client) candidateOrder() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, 0, len(c.order))
	if c.leader != "" {
		out = append(out, c.leader)
	}
	for _, id := range c.order {
		if id != c.leader {
			out = append(out, id)
		}
	}
	return out
}

func (c *Client) setLeader(id string) {
	c.mu.Lock()
	c.leader = id
	c.mu.Unlock()
}

// Close releases all connections.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, conn := range c.rawConn {
		_ = conn.Close()
	}
	return nil
}

// Put writes a key. It retries across nodes until the leader accepts it.
func (c *Client) Put(ctx context.Context, key string, value []byte) error {
	return c.retry(ctx, func(cl pb.KVServiceClient) (string, bool, error) {
		resp, err := cl.Put(ctx, &pb.PutRequest{Key: key, Value: value})
		if err != nil {
			return "", false, err
		}
		return resp.LeaderHint, resp.Ok, nil
	})
}

// Delete removes a key.
func (c *Client) Delete(ctx context.Context, key string) error {
	return c.retry(ctx, func(cl pb.KVServiceClient) (string, bool, error) {
		resp, err := cl.Delete(ctx, &pb.DeleteRequest{Key: key})
		if err != nil {
			return "", false, err
		}
		return resp.LeaderHint, resp.Ok, nil
	})
}

// Get reads a key from the leader. Returns (value, found, error).
func (c *Client) Get(ctx context.Context, key string) ([]byte, bool, error) {
	var value []byte
	var found bool
	err := c.retry(ctx, func(cl pb.KVServiceClient) (string, bool, error) {
		resp, err := cl.Get(ctx, &pb.GetRequest{Key: key})
		if err != nil {
			return "", false, err
		}
		if resp.LeaderHint != "" {
			return resp.LeaderHint, false, nil // redirect
		}
		value, found = resp.Value, resp.Found
		return "", true, nil
	})
	return value, found, err
}

// retry drives an operation across candidate nodes, honoring leader hints and
// backing off on transient errors.
func (c *Client) retry(ctx context.Context, op func(pb.KVServiceClient) (hint string, ok bool, err error)) error {
	backoff := 20 * time.Millisecond
	var lastErr error

	for attempt := 0; attempt < c.MaxRetries; attempt++ {
		for _, id := range c.candidateOrder() {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			cl, err := c.conn(id)
			if err != nil {
				lastErr = err
				continue
			}
			hint, ok, err := op(cl)
			if err != nil {
				lastErr = err
				continue // try next node
			}
			if ok {
				c.setLeader(id)
				return nil
			}
			// Not the leader: follow the hint if we got one.
			if hint != "" {
				c.setLeader(hint)
			}
			lastErr = fmt.Errorf("node %s is not leader (hint=%q)", id, hint)
			break // restart from (new) leader candidate
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < 500*time.Millisecond {
			backoff *= 2
		}
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("client: exhausted retries")
	}
	return lastErr
}
