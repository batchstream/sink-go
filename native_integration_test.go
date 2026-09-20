//go:build integration

package sink_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/liran/sink-go/internal/testuri"

	sink "github.com/liran/sink-go"
	"go.mongodb.org/mongo-driver/v2/bson"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func TestNativeCompatibility(t *testing.T) {
	target := os.Getenv("SINK_INTEGRATION_ADDRESS")
	if target == "" {
		t.Skip("SINK_INTEGRATION_ADDRESS is not set")
	}
	dialOptions := sink.DialOptions{TransportCredentials: insecure.NewCredentials()}
	client, err := sink.Dial(target, dialOptions)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if err := waitForHealth(ctx, client); err != nil {
		t.Fatalf("wait for Sink health: %v", err)
	}

	collection := fmt.Sprintf("native_%d", time.Now().UnixNano())
	program, err := sink.NewLuaProgram([]byte(`return function(current, incoming)
    current = current or {count = 0}
    current.count = current.count + incoming.count
    current.updated_at = incoming.updated_at
    return current
end`))
	if err != nil {
		t.Fatal(err)
	}
	opts := sink.DatasetOptions{URI: testuri.Resource("primary", []string{"sink_go_client", collection}), Encoding: sink.DocumentEncodingBSON, MergeProgram: &program}
	dataset, err := sink.NewDataset(client, opts)
	if err != nil {
		t.Fatal(err)
	}
	key := sink.StringKey("quota")
	address, err := sink.NewRecordAddress(opts.URI, key)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		deleteRequest := sink.DeleteRequest{
			CompletionMode: sink.CompletionWaitUntilApplied,
			Addresses:      []sink.Address{address},
		}
		_, _ = client.Delete(cleanup, deleteRequest)
	})
	value := struct {
		Count     int       `bson:"count"`
		UpdatedAt time.Time `bson:"updated_at"`
	}{Count: 1, UpdatedAt: integrationDateTime()}
	record := sink.Record{Key: key, Value: value, ReturnDocument: true}
	mergeRequest := sink.DatasetWriteRequest{
		CompletionMode: sink.CompletionWaitUntilApplied,
		Records:        []sink.Record{record, record},
	}
	results, err := dataset.Merge(ctx, mergeRequest)
	if err != nil || len(results) != 2 {
		t.Fatalf("returned Merge: %+v %v", results, err)
	}
	for index, result := range results {
		var stored struct {
			Count int `bson:"count"`
		}
		if err := result.Document.Decode(&stored); err != nil || stored.Count != index+1 {
			t.Fatalf("returned count=%d err=%v", stored.Count, err)
		}
	}
	indexDefinition := bson.D{{Key: "key", Value: bson.D{{Key: "count", Value: 1}}}, {Key: "name", Value: "count"}}
	indexArguments := bson.D{{Key: "indexes", Value: bson.A{indexDefinition}}}
	command, err := dataset.NewBSONCommand("createIndexes", indexArguments)
	if err != nil {
		t.Fatal(err)
	}
	native := sink.ExecuteRequest{Command: command}
	for range 2 {
		response, err := dataset.Execute(ctx, native)
		if err != nil || !response.Success || response.ContentType != "application/bson" {
			t.Fatalf("index setup: %+v %v", response, err)
		}
	}
	find := bson.D{{Key: "find", Value: ""}, {Key: "filter", Value: bson.D{{Key: "count", Value: 2}}}}
	command, err = sink.NewBSONCommand(opts.URI, find)
	if err != nil {
		t.Fatal(err)
	}
	native.Command = command
	if _, err := dataset.Execute(ctx, native); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Execute must reject cursor commands before execution: %v", err)
	}
	projection := &sink.Projection{Fields: []string{"count"}}
	query := sink.QueryRequest{Command: command, Page: 1, PageSize: 1,
		Sort: []sink.SortField{{Field: "count", Descending: true}, {Field: "_id"}}, Projection: projection}
	page, err := dataset.Query(ctx, query)
	if err != nil || len(page.Documents) != 1 || page.HasMore || bson.Raw(page.Documents[0].Payload()).Lookup("updated_at").Type != 0 {
		t.Fatalf("paged query=%+v err=%v", page, err)
	}
	countRequest := sink.CountRequest{Command: command}
	if count, err := dataset.Count(ctx, countRequest); err != nil || count.Count != 1 || count.Estimated {
		t.Fatalf("count=%+v err=%v", count, err)
	}
	all := sink.CountRequest{}
	result, err := dataset.Count(ctx, all)
	if err != nil || result.Count != 1 || !result.Estimated {
		t.Fatalf("full Dataset count=%+v err=%v", result, err)
	}
	allQuery := sink.QueryRequest{}
	if page, err := dataset.Query(ctx, allQuery); err != nil || len(page.Documents) != 1 {
		t.Fatalf("default Dataset query=%+v err=%v", page, err)
	}
	allScan := sink.ScanRequest{}
	if page, err := dataset.Scan(ctx, allScan); err != nil || len(page.Documents) != 1 {
		t.Fatalf("default Dataset scan=%+v err=%v", page, err)
	}
	all.Command.ContentType = "application/bson"
	if count, err := dataset.Count(ctx, all); err != nil || count.Count != 1 || !count.Estimated {
		t.Fatalf("default query with explicit encoding=%+v err=%v", count, err)
	}
	conflicts := []bson.D{
		{{Key: "find", Value: "other"}},
		{{Key: "find", Value: ""}, {Key: "find", Value: "other"}},
	}
	for _, conflict := range conflicts {
		conflicting, err := sink.NewBSONCommand(opts.URI, conflict)
		if err != nil {
			t.Fatal(err)
		}
		request := sink.QueryRequest{Command: conflicting}
		if _, err := dataset.Query(ctx, request); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("Store must reject conflicting native commands: %v", err)
		}
	}
	query.Page = 2
	if page, err := dataset.Query(ctx, query); err != nil || len(page.Documents) != 0 || page.HasMore {
		t.Fatalf("empty page=%+v err=%v", page, err)
	}
	scan := sink.ScanRequest{Command: command, BatchSize: 1}
	seen := 0
	visit := func(document sink.Document) error {
		seen++
		var stored struct {
			Count     int       `bson:"count"`
			UpdatedAt time.Time `bson:"updated_at"`
		}
		if err := document.Decode(&stored); err != nil {
			return err
		}
		if stored.Count != 2 || !stored.UpdatedAt.Equal(integrationDateTime()) {
			return fmt.Errorf("scan lost document or BSON datetime: %+v", stored)
		}
		return nil
	}
	scanPage, scanErr := dataset.Scan(ctx, scan)
	for _, document := range scanPage.Documents {
		if err := visit(document); err != nil {
			t.Fatal(err)
		}
	}
	if err := scanErr; err != nil || seen != 1 {
		t.Fatalf("scan seen=%d err=%v", seen, err)
	}
	scan.Projection = projection
	scanPage, err = dataset.Scan(ctx, scan)
	if err != nil || len(scanPage.Documents) != 1 {
		t.Fatalf("projected scan: %+v %v", scanPage, err)
	}
	rawScan := bson.Raw(scanPage.Documents[0].Payload())
	if rawScan.Lookup("count").AsInt64() != 2 || rawScan.Lookup("updated_at").Type != 0 {
		t.Fatalf("Scan projection was ignored: %s", rawScan)
	}
	invalid := bson.D{{Key: "count", Value: collection}, {Key: "unknownOption", Value: true}}
	command, err = sink.NewBSONCommand(opts.URI, invalid)
	if err != nil {
		t.Fatal(err)
	}
	native.Command = command
	response, err := dataset.Execute(ctx, native)
	var failure *sink.NativeError
	if !errors.As(err, &failure) || response.Success || bson.Raw(failure.Response.Payload).Lookup("errmsg").Type != bson.TypeString {
		t.Fatalf("native error lost: %+v %v", response, err)
	}
	modify := bson.D{{Key: "findAndModify", Value: collection}, {Key: "query", Value: bson.D{{Key: "_id", Value: "quota"}}},
		{Key: "update", Value: bson.D{{Key: "$inc", Value: bson.D{{Key: "count", Value: 1}}}}}, {Key: "new", Value: true}}
	command, err = sink.NewBSONCommand(opts.URI, modify)
	if err != nil {
		t.Fatal(err)
	}
	native.Command = command
	response, err = dataset.Execute(ctx, native)
	if err != nil || !response.Success || bson.Raw(response.Payload).Lookup("value", "count").AsInt64() != 3 {
		t.Fatalf("native mutation response=%+v err=%v", response, err)
	}
}
