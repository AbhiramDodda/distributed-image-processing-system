package raftgrpc

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

// peerQueue bounds each peer's outbound backlog. Past it, Send drops -- a slow or
// dead peer must never make the Raft run loop block, and Raft retransmits what is
// dropped.
const peerQueue = 1024

// stepTimeout caps one delivery so a hung peer can't wedge its sender goroutine.
const stepTimeout = 500 * time.Millisecond

// Transport implements consensus.Transport over gRPC. Each peer has its own
// bounded queue drained by a dedicated goroutine that lazily dials and reconnects,
// so Send is non-blocking and one unreachable peer never affects the others.
type Transport struct {
	from uint64
	log *slog.Logger
	peers map[uint64]*peerConn
	stop chan struct{}
	wg sync.WaitGroup
}

type peerConn struct {
	id uint64
	addr string
	ch chan []byte
}

// NewTransport builds a transport for node `from`, given the address of every
// peer (its own entry, if present, is ignored). Sender goroutines start
// immediately and dial lazily on first message.
func NewTransport(from uint64, peers map[uint64]string, log *slog.Logger) *Transport {
	if log == nil {
		log = slog.Default()
	}
	t := &Transport{
		from: from,
		log: log.With("component", "raft-transport", "node", from),
		peers: make(map[uint64]*peerConn),
		stop: make(chan struct{}),
	}
	for id, addr := range peers {
		if id == from {
			continue
		}
		pc := &peerConn{id: id, addr: addr, ch: make(chan []byte, peerQueue)}
		t.peers[id] = pc
		t.wg.Add(1)
		go t.sendLoop(pc)
	}
	return t
}

// Send marshals each message and enqueues it for its destination peer. It never
// blocks: an unknown destination or a full queue drops the message, and Raft
// resends. This is the same "drop rather than block" contract the in-process
// transport honours.
func (t *Transport) Send(msgs []*raftpb.Message) {
	for _, m := range msgs {
		if m.To == nil {
			continue
		}
		pc := t.peers[*m.To]
		if pc == nil {
			continue
		}
		data, err := proto.Marshal(m)
		if err != nil {
			continue
		}
		select {
		case pc.ch <- data:
		default:
			// Backlogged peer: drop. Raft will retransmit.
		}
	}
}

// Close stops all sender goroutines and blocks until they exit.
func (t *Transport) Close() {
	select {
	case <-t.stop:
	default:
		close(t.stop)
	}
	t.wg.Wait()
}

func (t *Transport) sendLoop(pc *peerConn) {
	defer t.wg.Done()

	var conn *grpc.ClientConn
	var client RaftTransportClient
	defer func() {
		if conn != nil {
			conn.Close()
		}
	}()

	// reset drops the current connection so the next send redials -- used after any
	// delivery error so a peer that restarts on a new connection is picked up.
	reset := func() {
		if conn != nil {
			conn.Close()
		}
		conn, client = nil, nil
	}

	for {
		select {
		case <-t.stop:
			return
		case data := <-pc.ch:
			if client == nil {
				// grpc.NewClient is lazy: it does not block here, it connects on the
				// first RPC. A construction error is rare (bad target) and non-fatal.
				c, err := grpc.NewClient(pc.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
				if err != nil {
					t.log.Warn("dial peer failed", "peer", pc.id, "addr", pc.addr, "err", err)
					continue
				}
				conn, client = c, NewRaftTransportClient(c)
			}
			ctx, cancel := context.WithTimeout(context.Background(), stepTimeout)
			_, err := client.Step(ctx, &RaftEnvelope{Data: data})
			cancel()
			if err != nil {
				reset()
			}
		}
	}
}
