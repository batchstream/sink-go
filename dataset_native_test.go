package sink_test

import (
	"bytes"
	"context"
	"google.golang.org/grpc"
	"net/http"
	"testing"
	"time"

	"github.com/batchstream/sink-go/internal/testuri"

	sink "github.com/batchstream/sink-go"
	sinkv1 "github.com/batchstream/sink-protocol/sink/v1"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type datasetNativeServer struct {
	queryRPCServer
	executes chan *sinkv1.ExecuteRequest
	scans    chan *sinkv1.ScanRequest
}

func (s *datasetNativeServer) Execute(_ context.Context, req *sinkv1.ExecuteRequest) (*sinkv1.ExecuteResponse, error) {
	s.executes <- req
	response := &sinkv1.ExecuteResponse{Success: true, ContentType: "application/json", Payload: []byte(`{"ok":true}`)}
	return response, nil
}

func (s *datasetNativeServer) scanResponse(_ context.Context, req *sinkv1.ScanRequest) (*sinkv1.ScanResponse, error) {
	s.scans <- req
	document := &sinkv1.Document{Encoding: sinkv1.DocumentEncoding_DOCUMENT_ENCODING_JSON, Payload: []byte(`{"number":1}`)}
	response := &sinkv1.ScanResponse{Documents: []*sinkv1.Document{document}}
	return response, nil
}

func TestDatasetNativeMethodsBindOpaqueURIAndRetainControls(t *testing.T) {
	for _, encoding := range []sink.DocumentEncoding{sink.DocumentEncodingBSON, sink.DocumentEncodingJSON} {
		t.Run(encoding.String(), func(t *testing.T) {
			server := &datasetNativeServer{executes: make(chan *sinkv1.ExecuteRequest, 1), scans: make(chan *sinkv1.ScanRequest, 1)}
			server.queries = make(chan *sinkv1.QueryRequest, 1)
			server.counts = make(chan *sinkv1.CountRequest, 1)
			clientOptions := sink.ClientOptions{}
			client := startTestClient(t, server, clientOptions)
			opts := sink.DatasetOptions{URI: "sink://primary/tenant/catalog/products", Encoding: encoding}
			dataset, err := sink.NewDataset(client, opts)
			if err != nil {
				t.Fatal(err)
			}
			projection := &sink.Projection{Fields: []string{"name"}}
			sortField := sink.SortField{Field: "name", Descending: true}
			query := sink.NewQueryRequest().WithPage(3).WithPageSize(1).
				WithSort(sortField).WithProjection(projection)
			if _, err := dataset.Query(t.Context(), query); err != nil {
				t.Fatal(err)
			}
			captured := <-server.queries
			assertDatasetNativeResource(t, captured.Command, opts.URI)
			if captured.Page != 3 || captured.PageSize != 1 || !captured.Sort[0].Descending || captured.Projection.Fields[0] != "name" || query.Command.URI != "" || len(query.Command.Payload) != 0 {
				t.Fatalf("query controls or caller request changed: %v %+v", captured, query)
			}
			count := sink.CountRequest{}
			if result, err := dataset.Count(t.Context(), count); err != nil || result.Count != 1<<53+1 || result.Estimated {
				t.Fatalf("count=%+v err=%v", result, err)
			}
			countRequest := <-server.counts
			assertDatasetNativeResource(t, countRequest.Command, opts.URI)
			scan := sink.NewScanRequest().WithBatchSize(23).
				WithCursor([]byte("checkpoint")).WithProjection(projection)
			if page, err := dataset.Scan(t.Context(), scan); err != nil || len(page.Documents) != 1 {
				t.Fatalf("scan=%+v err=%v", page, err)
			}
			scanRequest := <-server.scans
			assertDatasetNativeResource(t, scanRequest.Command, opts.URI)
			if scanRequest.BatchSize != 23 || string(scanRequest.Cursor) != "checkpoint" || scanRequest.GetProjection().GetFields()[0] != "name" {
				t.Fatal("batch size lost")
			}
			command := sink.Command{Method: "PUT", Path: "/_mapping", Query: "a=1&a=2", Payload: []byte(`{"properties":{}}`), Headers: http.Header{"Accept": {"application/json"}}}
			if encoding == sink.DocumentEncodingBSON {
				arguments := bson.D{{Key: "comment", Value: time.UnixMilli(123)}}
				command, err = dataset.NewBSONCommand("createIndexes", arguments)
				if err != nil {
					t.Fatal(err)
				}
			}
			execute := sink.ExecuteRequest{Command: command}
			if result, err := dataset.Execute(t.Context(), execute); err != nil || !result.Success {
				t.Fatalf("execute=%+v err=%v", result, err)
			}
			executed := (<-server.executes).Command
			if executed.Uri != opts.URI || !bytes.Equal(executed.Payload, command.Payload) {
				t.Fatalf("execute lost command: %v", executed)
			}
			if encoding == sink.DocumentEncodingJSON && (executed.Path != "/_mapping" || executed.Query != command.Query || executed.ContentType != "application/json" || len(executed.Headers) != 1 || command.Path != "/_mapping") {
				t.Fatalf("HTTP scope or controls lost: %v", executed)
			}
			if encoding == sink.DocumentEncodingBSON && (bson.Raw(executed.Payload).Lookup("createIndexes").StringValue() != "" || bson.Raw(executed.Payload).Lookup("comment").Type != bson.TypeDateTime) {
				t.Fatalf("BSON scope or type lost: %v", executed)
			}
		})
	}
}

func assertDatasetNativeResource(t *testing.T, command *sinkv1.Command, resource string) {
	t.Helper()
	if command.Uri != resource || command.Method != "" || command.Path != "" || command.ContentType != "" || len(command.Payload) != 0 {
		t.Fatalf("SDK changed the resource or supplied a backend query: %v", command)
	}
}

func TestDatasetNativeRejectsScopeConflictsAndInvalidCommands(t *testing.T) {
	server := &datasetNativeServer{}
	clientOptions := sink.ClientOptions{}
	client := startTestClient(t, server, clientOptions)
	for _, encoding := range []sink.DocumentEncoding{sink.DocumentEncodingBSON, sink.DocumentEncodingJSON} {
		opts := sink.DatasetOptions{URI: "sink://primary/tenant/catalog/products", Encoding: encoding}
		dataset, err := sink.NewDataset(client, opts)
		if err != nil {
			t.Fatal(err)
		}
		commands := []sink.Command{
			{URI: "sink://other"},
			{URI: "sink://primary/other"},
			{ContentType: "application/bson", Payload: []byte("invalid")},
			{ContentType: "invalid content type"},
		}
		for _, command := range commands {
			request := sink.QueryRequest{Command: command}
			if _, err := dataset.Query(t.Context(), request); err == nil {
				t.Fatalf("invalid scope accepted: %+v", command)
			}
		}
	}
	var dataset *sink.Dataset
	query := sink.QueryRequest{}
	count := sink.CountRequest{}
	execute := sink.ExecuteRequest{}
	scan := sink.ScanRequest{}
	if _, err := dataset.Query(t.Context(), query); err == nil {
		t.Fatal("nil Dataset Query accepted")
	}
	if _, err := dataset.Count(t.Context(), count); err == nil {
		t.Fatal("nil Dataset Count accepted")
	}
	if _, err := dataset.Execute(t.Context(), execute); err == nil {
		t.Fatal("nil Dataset Execute accepted")
	}
	if _, err := dataset.Scan(t.Context(), scan); err == nil {
		t.Fatal("nil Dataset Scan accepted")
	}
}

func TestCountPreservesAutomaticEstimateMetadata(t *testing.T) {
	for _, estimated := range []bool{false, true} {
		server := &queryRPCServer{estimated: estimated}
		opts := sink.ClientOptions{}
		client := startTestClient(t, server, opts)
		request := sink.CountRequest{Command: sdkNativeRequest().Command}
		result, err := client.Count(t.Context(), request)
		if err != nil || result.Estimated != estimated || result.Count != 1<<53+1 {
			t.Fatalf("count metadata lost: %+v %v", result, err)
		}
	}
}

func TestDatasetBSONPlaceholderPreservesTypesAndCallerBytes(t *testing.T) {
	server := &queryRPCServer{queries: make(chan *sinkv1.QueryRequest, 1)}
	clientOptions := sink.ClientOptions{}
	client := startTestClient(t, server, clientOptions)
	opts := sink.DatasetOptions{URI: testuri.Resource("primary", []string{"catalog", "products"}), Encoding: sink.DocumentEncodingBSON}
	dataset, err := sink.NewDataset(client, opts)
	if err != nil {
		t.Fatal(err)
	}
	id := bson.NewObjectID()
	filter := bson.D{{Key: "_id", Value: id}, {Key: "at", Value: time.UnixMilli(123)}, {Key: "n", Value: int64(1<<53 + 1)}}
	value := bson.D{{Key: "find", Value: ""}, {Key: "filter", Value: filter}}
	payload, err := bson.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	original := bytes.Clone(payload)
	command := sink.Command{Payload: payload}
	request := sink.QueryRequest{Command: command, PageSize: 1}
	if _, err := dataset.Query(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	captured := (<-server.queries).Command
	raw := bson.Raw(captured.Payload)
	if !bytes.Equal(payload, original) || !bytes.Equal(captured.Payload, original) || captured.Uri != opts.URI || raw.Lookup("find").StringValue() != "" || raw.Lookup("filter", "_id").ObjectID() != id || raw.Lookup("filter", "at").DateTime() != 123 || raw.Lookup("filter", "n").Int64() != 1<<53+1 {
		t.Fatalf("forwarding lost native BSON or changed caller: %s", raw)
	}
}

func TestDatasetNativePreservesExplicitEncodingAndStoreDefinedOperation(t *testing.T) {
	for _, encoding := range []sink.DocumentEncoding{sink.DocumentEncodingJSON, sink.DocumentEncodingBSON} {
		server := &datasetNativeServer{executes: make(chan *sinkv1.ExecuteRequest, 1)}
		clientOptions := sink.ClientOptions{}
		client := startTestClient(t, server, clientOptions)
		opts := sink.DatasetOptions{URI: "sink://custom", Encoding: encoding}
		dataset, err := sink.NewDataset(client, opts)
		if err != nil {
			t.Fatal(err)
		}
		command := sink.Command{URI: opts.URI, Method: "RUN", Path: "store-defined-operation", ContentType: "application/octet-stream", Payload: []byte{0, 1, 2}}
		request := sink.ExecuteRequest{Command: command}
		if _, err := dataset.Execute(t.Context(), request); err != nil {
			t.Fatal(err)
		}
		captured := (<-server.executes).Command
		if captured.Uri != opts.URI || captured.Method != command.Method || captured.Path != command.Path || captured.ContentType != command.ContentType || !bytes.Equal(captured.Payload, command.Payload) {
			t.Fatalf("SDK interpreted the Store's command: %v", captured)
		}
	}
}

func (s *datasetNativeServer) Scan(req *sinkv1.ScanRequest, stream grpc.ServerStreamingServer[sinkv1.ScanResponse]) error {
	response, err := s.scanResponse(stream.Context(), req)
	if err != nil {
		return err
	}
	if response == nil {
		return nil
	}
	for _, document := range response.Documents {
		frame := &sinkv1.ScanResponse{Documents: []*sinkv1.Document{document}}
		if err := stream.Send(frame); err != nil {
			return err
		}
	}
	final := &sinkv1.ScanResponse{Complete: true, NextCursor: response.NextCursor}
	return stream.Send(final)
}
