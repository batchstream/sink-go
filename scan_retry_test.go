package sink_test

import (
	"context"
	"testing"
	"time"

	sink "github.com/liran/sink-go"
	sinkv1 "github.com/liran/sink-go/api/sink/v1"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func scanAdmissionError(t *testing.T) error {
	t.Helper()
	detail := &errdetails.ErrorInfo{Domain: "sink", Reason: "SCAN_ADMISSION_REJECTED",
		Metadata: map[string]string{"pool": "execution", "reason": "wait_timeout"}}
	failure, err := status.New(codes.ResourceExhausted, "capacity is full").WithDetails(detail)
	if err != nil {
		t.Fatal(err)
	}
	return failure.Err()
}

func TestScanRetriesAdmissionWithIdenticalPage(t *testing.T) {
	server := &nativeRPCServer{scanFailures: 2, scanError: scanAdmissionError(t), scanRequests: make(chan *sinkv1.ScanRequest, 3)}
	policy := sink.RetryPolicy{InitialBackoff: time.Millisecond, MaxBackoff: time.Millisecond}
	opts := sink.ClientOptions{ScanRetry: policy}
	client := startTestClient(t, server, opts)
	projection := &sink.Projection{Fields: []string{"name"}}
	request := sink.ScanRequest{Command: sdkNativeRequest().Command, BatchSize: 2, Cursor: []byte("checkpoint"), Projection: projection}
	page, err := client.Scan(t.Context(), request)
	if err != nil || len(page.Documents) != 1 || len(page.NextCursor) != 0 || server.scanCalls.Load() != 3 {
		t.Fatalf("page=%+v calls=%d err=%v", page, server.scanCalls.Load(), err)
	}
	first := <-server.scanRequests
	for range 2 {
		if next := <-server.scanRequests; !proto.Equal(first, next) {
			t.Fatalf("retry changed the command or cursor: first=%v retry=%v", first, next)
		}
	}
	if string(request.Cursor) != "checkpoint" {
		t.Fatal("retry advanced caller checkpoint")
	}
}

func TestScanAdmissionRetriesAreBoundedOrDisabled(t *testing.T) {
	for _, attempts := range []int{1, 3} {
		server := &nativeRPCServer{scanFailures: 100, scanError: scanAdmissionError(t)}
		policy := sink.RetryPolicy{MaxAttempts: attempts, InitialBackoff: time.Millisecond, MaxBackoff: time.Millisecond}
		opts := sink.ClientOptions{ScanRetry: policy}
		client := startTestClient(t, server, opts)
		request := sink.ScanRequest{Command: sdkNativeRequest().Command, Cursor: []byte("checkpoint")}
		page, err := client.Scan(t.Context(), request)
		if status.Code(err) != codes.ResourceExhausted || server.scanCalls.Load() != int32(attempts) || len(page.Documents) != 0 || len(page.NextCursor) != 0 {
			t.Fatalf("attempt limit=%d page=%+v calls=%d err=%v", attempts, page, server.scanCalls.Load(), err)
		}
		if len(status.Convert(err).Details()) != 1 || string(request.Cursor) != "checkpoint" {
			t.Fatal("failed retry lost status detail or changed checkpoint")
		}
	}
}

func TestScanDoesNotRetryUnmarkedOrOtherFailures(t *testing.T) {
	wrongDomain := &errdetails.ErrorInfo{Domain: "backend", Reason: "SCAN_ADMISSION_REJECTED"}
	wrongReason := &errdetails.ErrorInfo{Domain: "sink", Reason: "SOME_OTHER_ERROR"}
	admission := &errdetails.ErrorInfo{Domain: "sink", Reason: "SCAN_ADMISSION_REJECTED"}
	cases := []struct {
		code   codes.Code
		detail *errdetails.ErrorInfo
	}{
		{code: codes.ResourceExhausted},
		{code: codes.ResourceExhausted, detail: wrongDomain},
		{code: codes.ResourceExhausted, detail: wrongReason},
		{code: codes.Unavailable, detail: admission},
		{code: codes.InvalidArgument},
		{code: codes.DeadlineExceeded},
	}
	for _, test := range cases {
		failure := status.New(test.code, "Sink execution capacity is full")
		if test.detail != nil {
			var err error
			failure, err = failure.WithDetails(test.detail)
			if err != nil {
				t.Fatal(err)
			}
		}
		server := &nativeRPCServer{scanFailures: 100, scanError: failure.Err()}
		opts := sink.ClientOptions{}
		client := startTestClient(t, server, opts)
		request := sink.ScanRequest{Command: sdkNativeRequest().Command}
		_, err := client.Scan(t.Context(), request)
		if status.Code(err) != test.code || server.scanCalls.Load() != 1 {
			t.Fatalf("unexpected retry: calls=%d err=%v", server.scanCalls.Load(), err)
		}
	}
}

func TestScanRetryBackoffHonorsCancellationAndTotalTimeout(t *testing.T) {
	for _, mode := range []string{"cancel", "caller_timeout", "scan_timeout"} {
		t.Run(mode, func(t *testing.T) {
			server := &nativeRPCServer{scanFailures: 100, scanError: scanAdmissionError(t), scanRequests: make(chan *sinkv1.ScanRequest, 100)}
			policy := sink.RetryPolicy{MaxAttempts: 100, InitialBackoff: time.Second, MaxBackoff: time.Second}
			opts := sink.ClientOptions{ScanRetry: policy}
			if mode == "scan_timeout" {
				opts.ScanTimeout = 100 * time.Millisecond
			}
			client := startTestClient(t, server, opts)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if mode == "caller_timeout" {
				var timeoutCancel context.CancelFunc
				ctx, timeoutCancel = context.WithTimeout(ctx, 100*time.Millisecond)
				defer timeoutCancel()
			}
			request := sink.ScanRequest{Command: sdkNativeRequest().Command, Cursor: []byte("checkpoint")}
			finished := make(chan error, 1)
			go func() { _, err := client.Scan(ctx, request); finished <- err }()
			select {
			case <-server.scanRequests:
			case <-time.After(time.Second):
				t.Fatal("first attempt did not reach server")
			}
			if mode == "cancel" {
				cancel()
			}
			select {
			case err := <-finished:
				want := codes.DeadlineExceeded
				if mode == "cancel" {
					want = codes.Canceled
				}
				if status.Code(err) != want || server.scanCalls.Load() != 1 || string(request.Cursor) != "checkpoint" {
					t.Fatalf("calls=%d err=%v", server.scanCalls.Load(), err)
				}
			case <-time.After(time.Second):
				t.Fatal("backoff ignored cancellation or page timeout")
			}
		})
	}
}
