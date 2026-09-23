package sink_test

import (
	"context"
	"google.golang.org/grpc"
	"testing"
	"time"

	sink "github.com/batchstream/sink-go"
	sinkv1 "github.com/batchstream/sink-protocol/sink/v1"
)

type scanDeadlineServer struct {
	sinkv1.UnimplementedSinkServer
	deadlines chan time.Time
}

func (s *scanDeadlineServer) scanResponse(ctx context.Context, _ *sinkv1.ScanRequest) (*sinkv1.ScanResponse, error) {
	deadline, _ := ctx.Deadline()
	s.deadlines <- deadline
	response := &sinkv1.ScanResponse{}
	return response, nil
}

func TestScanDeadlineBelongsToCaller(t *testing.T) {
	for _, mode := range []string{"none", "context", "explicit_option"} {
		t.Run(mode, func(t *testing.T) {
			server := &scanDeadlineServer{deadlines: make(chan time.Time, 1)}
			opts := sink.ClientOptions{}
			if mode == "explicit_option" {
				opts.ScanTimeout = time.Hour
			}
			client := startTestClient(t, server, opts)
			ctx := context.Background()
			if mode == "context" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, time.Hour)
				defer cancel()
			}
			request := sink.ScanRequest{Command: sdkNativeRequest().Command}
			if _, err := client.Scan(ctx, request); err != nil {
				t.Fatal(err)
			}
			got := <-server.deadlines
			if mode == "none" && !got.IsZero() {
				t.Fatalf("default Scan invented a deadline: %v", got)
			}
			if mode != "none" && (got.IsZero() || time.Until(got) < 59*time.Minute) {
				t.Fatalf("caller hour shortened: %v", got)
			}
		})
	}
}

func (s *scanDeadlineServer) Scan(req *sinkv1.ScanRequest, stream grpc.ServerStreamingServer[sinkv1.ScanResponse]) error {
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
