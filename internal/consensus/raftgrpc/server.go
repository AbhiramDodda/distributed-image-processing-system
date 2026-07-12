// Package raftgrpc is a gRPC Transport for the consensus package, so a Raft
// cluster can run across separate coordinator processes rather than only in one
// process (the InmemNetwork test harness). It is a dumb byte pipe: the consensus
// layer marshals a raftpb.Message, this package ships it to the destination peer,
// and the peer's server hands it back to its Node. Delivery is best-effort --
// Raft retransmits -- so the transport may drop rather than block.
package raftgrpc

import (
	"context"

	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// Receiver is the minimal surface the server needs to deliver an inbound message:
// somewhere to hand a decoded raftpb.Message. *consensus.Node satisfies it via
// its Receive method, so this package need not import consensus (no import cycle,
// and it stays unit-testable with a fake receiver).
type Receiver interface {
	Receive(m *raftpb.Message)
}

// Server implements the RaftTransport gRPC service by decoding each envelope and
// forwarding it to the local Raft node. Register it on a coordinator's raft gRPC
// listener with RegisterRaftTransportServer.
type Server struct {
	UnimplementedRaftTransportServer
	node Receiver
}

// NewServer returns a Server that delivers inbound messages to node.
func NewServer(node Receiver) *Server { return &Server{node: node} }

// Step decodes the marshaled raftpb.Message and hands it to the local node. A
// malformed payload is reported as an error so the sender can drop it; Node.Receive
// itself never blocks, so a burst from a peer cannot stall this handler.
func (s *Server) Step(ctx context.Context, env *RaftEnvelope) (*RaftAck, error) {
	var m raftpb.Message
	if err := proto.Unmarshal(env.GetData(), &m); err != nil {
		return nil, err
	}
	s.node.Receive(&m)
	return &RaftAck{}, nil
}
