package sink_test

import (
	"errors"
	"fmt"
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
	readRequest := sink.NewReadRequest(addresses...).WithOnResult(func(result sink.ReadResult) error {
		seen[result.OperationIndex]++
		return nil
	})
	results, err := client.Read(t.Context(), readRequest)
	if err != nil || results != nil || len(seen) != 3 || server.reads.Load() != 2 {
		t.Fatalf("results=%v seen=%v calls=%d err=%v", results, seen, server.reads.Load(), err)
	}
	for _, count := range seen {
		if count != 1 {
			t.Fatal("callback redelivered a completed read")
		}
	}
	// Collecting uses the same stream, restores request order, and retains payloads.
	readRequest2 := readRequest.WithOnResult(nil)
	if readRequest.OnResult == nil {
		t.Fatal("WithOnResult modified the original request")
	}
	results, err = client.Read(t.Context(), readRequest2)
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
	readRequest := sink.ReadRequest{
		Addresses: addresses,
		OnResult:  func(sink.ReadResult) error { return stop },
	}
	results, err := client.Read(t.Context(), readRequest)
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
		writeRequest := sink.NewWriteRequest(operations...).WithOnResult(emit)
		results, err := client.Write(t.Context(), writeRequest)
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
		scanRequest := request
		scanRequest.OnDocument = func(sink.Document) error { count++; return nil }
		response, err := client.Scan(t.Context(), scanRequest)
		if status.Code(err) != codes.Unavailable || count != 1 || response.Documents != nil || len(response.NextCursor) != 0 {
			t.Fatalf("unsafe cursor/collection: %+v %v count=%d", response, err, count)
		}
	}
}

func TestDatasetRequestCallbacksPreserveBatchFailuresWithoutCollecting(t *testing.T) {
	server := &testSinkServer{}
	opts := sink.ClientOptions{MaxOperations: 2}
	client := startTestClient(t, server, opts)
	datasetOptions := sink.DatasetOptions{URI: "sink://primary/items", Encoding: sink.DocumentEncodingJSON}
	dataset, err := sink.NewDataset(client, datasetOptions)
	if err != nil {
		t.Fatal(err)
	}
	keys := []sink.Key{sink.StringKey("a"), sink.StringKey("b"), sink.StringKey("c")}
	readIndexes := make(map[int]int)
	readRequest := sink.NewDatasetReadRequest(keys...).WithOnResult(func(result sink.ReadResult) error {
		readIndexes[result.OperationIndex]++
		return result.Err()
	})
	readResults, err := dataset.Read(t.Context(), readRequest)
	if err != nil || readResults != nil || len(readIndexes) != len(keys) {
		t.Fatalf("read callbacks: results=%v indexes=%v err=%v", readResults, readIndexes, err)
	}
	for index := range keys {
		if readIndexes[index] != 1 {
			t.Fatalf("read index %d delivered %d times", index, readIndexes[index])
		}
	}
	records := make([]sink.Record, len(keys))
	for index, key := range keys {
		records[index] = sink.Record{Key: key, Value: map[string]int{"value": index}}
	}
	writeIndexes := make(map[int]int)
	writeRequest := sink.NewDatasetWriteRequest(records...).WithOnResult(func(result sink.WriteResult) error {
		writeIndexes[result.OperationIndex]++
		return nil
	})
	writeResults, err := dataset.Upsert(t.Context(), writeRequest)
	var batchError *sink.BatchError
	if writeResults != nil || len(writeIndexes) != len(records) || !errors.As(err, &batchError) {
		t.Fatalf("write callbacks: results=%v indexes=%v err=%v", writeResults, writeIndexes, err)
	}
	if len(batchError.Failures) != 1 || batchError.Failures[0].OperationIndex != 1 {
		t.Fatalf("callback lost operation failure: %+v", batchError.Failures)
	}
	for index := range records {
		if writeIndexes[index] != 1 {
			t.Fatalf("write index %d delivered %d times", index, writeIndexes[index])
		}
	}
}

func TestNativeRequestCallbacksKeepMetadataWithoutCollecting(t *testing.T) {
	for _, scoped := range []bool{false, true} {
		t.Run(fmt.Sprintf("dataset=%t", scoped), func(t *testing.T) {
			queryServer := &queryRPCServer{}
			opts := sink.ClientOptions{}
			queryClient := startTestClient(t, queryServer, opts)
			scanServer := &nativeRPCServer{}
			scanClient := startTestClient(t, scanServer, opts)
			command := sdkNativeRequest().Command
			datasetOptions := sink.DatasetOptions{URI: command.URI, Encoding: sink.DocumentEncodingJSON}
			queryDataset, err := sink.NewDataset(queryClient, datasetOptions)
			if err != nil {
				t.Fatal(err)
			}
			scanDataset, err := sink.NewDataset(scanClient, datasetOptions)
			if err != nil {
				t.Fatal(err)
			}
			queryCount := 0
			queryRequest := sink.NewQueryRequest().WithCommand(command).WithPageSize(1).
				WithOnDocument(func(document sink.Document) error {
					queryCount++
					return nil
				})
			var queryPage sink.QueryResponse
			if scoped {
				queryPage, err = queryDataset.Query(t.Context(), queryRequest)
			} else {
				queryPage, err = queryClient.Query(t.Context(), queryRequest)
			}
			if err != nil || queryCount != 1 || queryPage.Documents != nil || !queryPage.HasMore {
				t.Fatalf("query callback: page=%+v count=%d err=%v", queryPage, queryCount, err)
			}
			scanCount := 0
			scanRequest := sink.NewScanRequest().WithCommand(command).
				WithOnDocument(func(document sink.Document) error {
					scanCount++
					return nil
				})
			var scanPage sink.ScanResponse
			if scoped {
				scanPage, err = scanDataset.Scan(t.Context(), scanRequest)
			} else {
				scanPage, err = scanClient.Scan(t.Context(), scanRequest)
			}
			if err != nil || scanCount != 1 || scanPage.Documents != nil || string(scanPage.NextCursor) != "next" {
				t.Fatalf("scan callback: page=%+v count=%d err=%v", scanPage, scanCount, err)
			}
		})
	}
}
