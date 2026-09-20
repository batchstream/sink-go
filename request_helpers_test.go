package sink_test

import (
	"testing"

	sink "github.com/liran/sink-go"
	sinkv1 "github.com/liran/sink-go/api/sink/v1"
)

func TestRequestCompletionDefaultsAndOverridesReachRPC(t *testing.T) {
	cases := []struct {
		name    string
		mode    sink.CompletionMode
		helpers bool
		invalid bool
	}{
		{name: "literal defaults"},
		{name: "helper defaults", helpers: true},
		{name: "applied", mode: sink.CompletionWaitUntilApplied, helpers: true},
		{name: "accepted", mode: sink.CompletionReturnAfterAccepted, helpers: true},
		{name: "visible", mode: sink.CompletionWaitUntilVisible, helpers: true},
		{name: "negative", mode: -1, helpers: true, invalid: true},
		{name: "unknown", mode: 99, helpers: true, invalid: true},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			server := &testSinkServer{}
			options := sink.ClientOptions{}
			client := startTestClient(t, server, options)
			address := testAddress(t, sink.StringKey("defaults"))
			operation, err := sink.NewPut(address, testDocument("value"), sink.WriteUpsert)
			if err != nil {
				t.Fatal(err)
			}
			datasetOptions := sink.DatasetOptions{URI: "sink://primary/items", Encoding: sink.DocumentEncodingJSON}
			dataset, err := sink.NewDataset(client, datasetOptions)
			if err != nil {
				t.Fatal(err)
			}
			record := sink.Record{Key: sink.StringKey("defaults"), Value: map[string]int{"value": 1}}
			writeRequest := sink.WriteRequest{Operations: []sink.WriteOperation{operation}}
			deleteRequest := sink.DeleteRequest{Addresses: []sink.Address{address}}
			datasetRequest := sink.DatasetWriteRequest{Records: []sink.Record{record}}
			if test.helpers {
				writeRequest = sink.NewWriteRequest(operation)
				deleteRequest = sink.NewDeleteRequest(address)
				datasetRequest = sink.NewDatasetWriteRequest(record)
				if test.mode != 0 {
					writeRequest = writeRequest.WithCompletionMode(test.mode)
					deleteRequest = deleteRequest.WithCompletionMode(test.mode)
					datasetRequest = datasetRequest.WithCompletionMode(test.mode)
				}
			}
			writeResults, writeErr := client.Write(t.Context(), writeRequest)
			deleteResults, deleteErr := client.Delete(t.Context(), deleteRequest)
			datasetResults, datasetErr := dataset.Upsert(t.Context(), datasetRequest)
			if test.invalid {
				_, writes, deletes := server.counts()
				if writeErr == nil || deleteErr == nil || datasetErr == nil || writes != 0 || deletes != 0 {
					t.Fatalf("invalid mode reached RPC: write=%v delete=%v dataset=%v calls=%d/%d", writeErr, deleteErr, datasetErr, writes, deletes)
				}
				return
			}
			if writeErr != nil || deleteErr != nil || datasetErr != nil {
				t.Fatalf("write=%v delete=%v dataset=%v", writeErr, deleteErr, datasetErr)
			}
			if len(writeResults) != 1 || len(deleteResults) != 1 || len(datasetResults) != 1 {
				t.Fatal("default collection lost results")
			}
			want := test.mode
			if want == 0 {
				want = sink.CompletionWaitUntilApplied
			}
			server.mu.Lock()
			defer server.mu.Unlock()
			for _, request := range server.writeRequests {
				if request.CompletionMode != want {
					t.Fatalf("write completion = %v, want %v", request.CompletionMode, want)
				}
			}
			if server.deleteRequest.CompletionMode != want {
				t.Fatalf("delete completion = %v, want %v", server.deleteRequest.CompletionMode, want)
			}
			if !test.helpers && (writeRequest.CompletionMode != 0 || deleteRequest.CompletionMode != 0 || datasetRequest.CompletionMode != 0) {
				t.Fatal("applying defaults mutated the caller request")
			}
		})
	}
}

func TestNativeRequestDefaultsReachRPC(t *testing.T) {
	for _, helpers := range []bool{false, true} {
		queryServer := &queryRPCServer{queries: make(chan *sinkv1.QueryRequest, 1), lastPage: true}
		scanServer := &nativeRPCServer{scanRequests: make(chan *sinkv1.ScanRequest, 1)}
		options := sink.ClientOptions{}
		queryClient := startTestClient(t, queryServer, options)
		scanClient := startTestClient(t, scanServer, options)
		command := sdkNativeRequest().Command
		queryRequest := sink.QueryRequest{Command: command}
		scanRequest := sink.ScanRequest{Command: command}
		if helpers {
			queryRequest = sink.NewQueryRequest().WithCommand(command)
			scanRequest = sink.NewScanRequest().WithCommand(command)
		}
		page, err := queryClient.Query(t.Context(), queryRequest)
		if err != nil || len(page.Documents) != 1 || page.HasMore {
			t.Fatalf("default query: %+v %v", page, err)
		}
		queryWire := <-queryServer.queries
		if queryWire.Page != 1 || queryWire.PageSize != 100 || queryWire.Projection != nil || len(queryWire.Sort) != 0 {
			t.Fatalf("unexpected query defaults: %v", queryWire)
		}
		scanPage, err := scanClient.Scan(t.Context(), scanRequest)
		if err != nil || len(scanPage.Documents) != 1 || string(scanPage.NextCursor) != "next" {
			t.Fatalf("default scan: %+v %v", scanPage, err)
		}
		scanWire := <-scanServer.scanRequests
		if scanWire.BatchSize != 100 || len(scanWire.Cursor) != 0 || scanWire.Projection != nil {
			t.Fatalf("unexpected scan defaults: %v", scanWire)
		}
		if !helpers && (queryRequest.Page != 0 || queryRequest.PageSize != 0 || scanRequest.BatchSize != 0) {
			t.Fatal("applying defaults mutated the caller request")
		}
	}
}
