package sink_test

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	sink "github.com/liran/sink-go"
	sinkv1 "github.com/liran/sink-go/api/sink/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type partialStreamServer struct {
	sinkv1.UnimplementedSinkServer
	reads    atomic.Int32
	writes   atomic.Int32
	wait     bool
	canceled chan struct{}
}

func (s *partialStreamServer) Read(req *sinkv1.ReadRequest, stream grpc.ServerStreamingServer[sinkv1.ReadResponse]) error {
	call := s.reads.Add(1)
	for i := len(req.Operations) - 1; i >= 0; i-- {
		document := &sinkv1.Document{Encoding: sinkv1.DocumentEncoding_DOCUMENT_ENCODING_JSON, Payload: []byte(`{"value":1}`)}
		result := &sinkv1.ReadResult{OperationIndex: uint32(i), Status: sinkv1.ReadStatus_READ_STATUS_FOUND, Document: document}
		frame := &sinkv1.ReadResponse{Results: []*sinkv1.ReadResult{result}}
		if err := stream.Send(frame); err != nil {
			return err
		}
		if s.wait {
			<-stream.Context().Done()
			close(s.canceled)
			return stream.Context().Err()
		}
		if call == 1 {
			return status.Error(codes.Unavailable, "interrupted after one result")
		}
	}
	return nil
}
func (s *partialStreamServer) Write(_ *sinkv1.WriteRequest, stream grpc.ServerStreamingServer[sinkv1.WriteResponse]) error {
	s.writes.Add(1)
	result := &sinkv1.WriteResult{OperationIndex: 1, Status: sinkv1.WriteStatus_WRITE_STATUS_APPLIED}
	frame := &sinkv1.WriteResponse{Results: []*sinkv1.WriteResult{result}}
	if err := stream.Send(frame); err != nil {
		return err
	}
	return status.Error(codes.Unavailable, "write outcome unknown")
}
func TestStreamingReadRetriesOnlyUndeliveredAndDoesNotCollectCallbacks(t *testing.T) {
	server := &partialStreamServer{}
	opts := sink.ClientOptions{}
	client := startTestClient(t, server, opts)
	addresses := []sink.Address{testAddress(t, sink.StringKey("a")), testAddress(t, sink.StringKey("b")), testAddress(t, sink.StringKey("c"))}
	seen := make(map[int]int)
	results, err := client.Read(t.Context(), addresses, func(result sink.ReadResult) error { seen[result.OperationIndex]++; return nil })
	if err != nil || results != nil || len(seen) != 3 || server.reads.Load() != 2 {
		t.Fatalf("results=%v seen=%v calls=%d err=%v", results, seen, server.reads.Load(), err)
	}
	for _, count := range seen {
		if count != 1 {
			t.Fatal("callback redelivered a completed read")
		}
	}
	// Collecting uses the same stream, restores request order, and retains payloads.
	results, err = client.Read(t.Context(), addresses)
	if err != nil || len(results) != 3 {
		t.Fatalf("collect: %v %v", results, err)
	}
	for i, result := range results {
		if result.OperationIndex != i {
			t.Fatal("collecting changed request order")
		}
	}
}
func TestCallbackErrorCancelsWithoutRetry(t *testing.T) {
	server := &partialStreamServer{wait: true, canceled: make(chan struct{})}
	opts := sink.ClientOptions{}
	client := startTestClient(t, server, opts)
	addresses := []sink.Address{testAddress(t, sink.StringKey("a"))}
	stop := status.Error(codes.Unavailable, "callback stopped")
	results, err := client.Read(t.Context(), addresses, func(sink.ReadResult) error { return stop })
	if !errors.Is(err, stop) || results != nil || server.reads.Load() != 1 {
		t.Fatalf("callback cancellation: %v %v", results, err)
	}
	select {
	case <-server.canceled:
	case <-time.After(time.Second):
		t.Fatal("callback did not cancel the server stream")
	}
}
func TestStreamingWritePreservesPartialResultsAndNeverReplays(t *testing.T) {
	for _, callback := range []bool{false, true} {
		server := &partialStreamServer{}
		opts := sink.ClientOptions{}
		client := startTestClient(t, server, opts)
		address := testAddress(t, sink.StringKey("a"))
		operation, err := sink.NewPut(address, testDocument("x"), sink.WriteUpsert)
		if err != nil {
			t.Fatal(err)
		}
		operations := []sink.WriteOperation{operation, operation}
		var emit sink.WriteCallback
		count := 0
		if callback {
			emit = func(result sink.WriteResult) error {
				count++
				if result.OperationIndex != 1 {
					t.Error("lost global index")
				}
				return nil
			}
		}
		results, err := client.Write(t.Context(), sink.CompletionWaitUntilApplied, operations, emit)
		if status.Code(err) != codes.Unavailable || server.writes.Load() != 1 {
			t.Fatalf("write replay/status: %v", err)
		}
		if callback {
			if results != nil || count != 1 {
				t.Fatal("callback write retained results")
			}
		} else if len(results) != 1 || results[0].OperationIndex != 1 {
			t.Fatal("lost acknowledged write")
		}
	}
}

type incompletePageServer struct {
	sinkv1.UnimplementedSinkServer
	afterCompletion bool
}

func (s *incompletePageServer) Scan(_ *sinkv1.ScanRequest, stream grpc.ServerStreamingServer[sinkv1.ScanResponse]) error {
	document := &sinkv1.Document{Encoding: sinkv1.DocumentEncoding_DOCUMENT_ENCODING_JSON, Payload: []byte(`{"x":1}`)}
	frame := &sinkv1.ScanResponse{Documents: []*sinkv1.Document{document}}
	if err := stream.Send(frame); err != nil {
		return err
	}
	if s.afterCompletion {
		final := &sinkv1.ScanResponse{Complete: true, NextCursor: []byte("uncommitted-cursor")}
		if err := stream.Send(final); err != nil {
			return err
		}
	}
	return status.Error(codes.Unavailable, "lost page completion")
}
func TestStreamingScanWithholdsCursorOnFailure(t *testing.T) {
	for _, final := range []bool{false, true} {
		server := &incompletePageServer{afterCompletion: final}
		opts := sink.ClientOptions{}
		client := startTestClient(t, server, opts)
		request := sink.ScanRequest{Command: sink.Command{URI: "sink://primary/items"}}
		count := 0
		response, err := client.Scan(t.Context(), request, func(sink.Document) error { count++; return nil })
		if status.Code(err) != codes.Unavailable || count != 1 || response.Documents != nil || len(response.NextCursor) != 0 {
			t.Fatalf("unsafe cursor/collection: %+v %v count=%d", response, err, count)
		}
	}
}
