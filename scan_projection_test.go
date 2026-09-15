package sink_test

import (
	"reflect"
	"testing"

	sink "github.com/liran/sink-go"
	sinkv1 "github.com/liran/sink-go/api/sink/v1"
)

func TestScanProjectionWirePresenceAndValidation(t *testing.T) {
	server := &nativeRPCServer{scanRequests: make(chan *sinkv1.ScanRequest, 1)}
	opts := sink.ClientOptions{}
	client := startTestClient(t, server, opts)
	request := sink.ScanRequest{Command: sdkNativeRequest().Command}
	for _, projection := range []*sink.Projection{nil, {}, {Fields: []string{"name", "nested.value"}}, {Fields: []string{"_id"}, Exclude: true}} {
		request.Projection = projection
		if _, err := client.Scan(t.Context(), request); err != nil {
			t.Fatal(err)
		}
		captured := (<-server.scanRequests).GetProjection()
		if (captured == nil) != (projection == nil) {
			t.Fatal("projection presence changed")
		}
		if projection != nil && (!reflect.DeepEqual(captured.Fields, projection.Fields) || captured.Exclude != projection.Exclude) {
			t.Fatalf("projection lost: %+v", captured)
		}
	}
	calls := server.scanCalls.Load()
	for _, fields := range [][]string{{""}, {" "}, {"name", "name"}, {"invalid\xff"}} {
		request.Projection = &sink.Projection{Fields: fields}
		if _, err := client.Scan(t.Context(), request); err == nil {
			t.Fatalf("invalid projection accepted: %q", fields)
		}
	}
	if server.scanCalls.Load() != calls {
		t.Fatal("invalid projection reached the RPC")
	}
}
