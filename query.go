package sink

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"

	sinkv1 "github.com/liran/sink-go/api/sink/v1"
)

type QueryRequest struct {
	Command    Command
	Page       int         // One-based; zero selects page 1.
	PageSize   int         // Zero selects 100; maximum 1000.
	Sort       []SortField // Ordered keys; empty preserves native sorting.
	Projection *Projection // Nil preserves the native projection.
	// OnDocument consumes each document without collecting it in the response.
	// Nil collects documents. Returning an error cancels the stream.
	OnDocument DocumentCallback
}

type SortField struct {
	Field      string
	Descending bool
}

// Projection includes or excludes native document paths. An empty Fields list
// selects all fields. Search paths are relative to _source; hit metadata remains.
type Projection struct {
	Fields  []string
	Exclude bool
}

type QueryResponse struct {
	Documents []Document
	HasMore   bool
}

// Query fetches an independent page without retaining a cursor between calls.
// Use a stable native sort with a unique tie-breaker. Concurrent writes can shift
// pages; deep pages are subject to backend offset costs and result-window limits.
// HasMore uses one extra result. Query never automatically runs Count or retries.
// An optional callback receives documents without collecting them in Documents.
// HasMore is valid only when the stream finishes successfully.
func (c *Client) Query(ctx context.Context, req QueryRequest) (QueryResponse, error) {
	var empty QueryResponse
	if c == nil || c.rpc == nil {
		return empty, errors.New("query requires a client")
	}
	if req.Page == 0 {
		req.Page = 1
	}
	if req.PageSize == 0 {
		req.PageSize = defaultPageSize
	}
	if req.Page < 0 || uint64(req.Page) > uint64(^uint32(0)) || req.PageSize < 0 || req.PageSize > 1000 {
		return empty, errors.New("query requires a nonnegative uint32 page and page size between 0 and 1000")
	}
	command, err := req.Command.toProto()
	if err != nil {
		return empty, err
	}
	request := &sinkv1.QueryRequest{Command: command, Page: uint32(req.Page), PageSize: uint32(req.PageSize)}
	seen := make(map[string]bool)
	for _, field := range req.Sort {
		if strings.TrimSpace(field.Field) == "" || seen[field.Field] {
			return empty, errors.New("sort fields must be nonempty and unique")
		}
		seen[field.Field] = true
		item := &sinkv1.SortField{Field: field.Field, Descending: field.Descending}
		request.Sort = append(request.Sort, item)
	}
	request.Projection, err = req.Projection.toProto()
	if err != nil {
		return empty, err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := c.rpc.Query(ctx, request, c.config.sinkCallOptions...)
	if err != nil {
		return empty, fmt.Errorf("query native page: %w", err)
	}
	pageSize := req.PageSize
	result := QueryResponse{}
	complete, hasMore, count := false, false, 0
	for {
		frame, err := stream.Recv()
		if err == io.EOF {
			if !complete {
				return result, protocolError("Query", "stream omitted completion")
			}
			result.HasMore = hasMore
			return result, nil
		}
		if err != nil {
			return result, fmt.Errorf("query native page: %w", err)
		}
		if frame == nil || complete {
			return result, protocolError("Query", "unexpected frame after completion")
		}
		if frame.GetComplete() {
			if len(frame.Documents) != 0 || (frame.GetHasMore() && count != pageSize) {
				return result, protocolError("Query", "invalid page completion")
			}
			complete, hasMore = true, frame.GetHasMore()
			continue
		}
		if len(frame.Documents) != 1 || frame.GetHasMore() || count >= pageSize {
			return result, protocolError("Query", "invalid document frame")
		}
		document, err := documentFromProto(frame.Documents[0])
		if err != nil {
			return result, protocolError("Query", err.Error())
		}
		count++
		if req.OnDocument != nil {
			if err := req.OnDocument(document); err != nil {
				return result, err
			}
		} else {
			result.Documents = append(result.Documents, document)
		}
	}
}

type CountRequest struct {
	Command Command
}

type CountResponse struct {
	Count     uint64
	Estimated bool // True when the backend used collection metadata.
}

// Count counts matches before find/HTTP pagination, or after the supplied
// aggregate pipeline. MongoDB automatically uses metadata for ordinary empty
// find filters; other queries remain exact. Estimated identifies the strategy.
// Concurrent writes can make the count differ from a separately fetched page.
// The SDK never retries.
func (c *Client) Count(ctx context.Context, req CountRequest) (CountResponse, error) {
	var empty CountResponse
	if c == nil || c.rpc == nil {
		return empty, errors.New("count requires a client")
	}
	command, err := req.Command.toProto()
	if err != nil {
		return empty, err
	}
	request := &sinkv1.CountRequest{Command: command}
	response, err := c.rpc.Count(ctx, request, c.config.sinkCallOptions...)
	if err != nil {
		return empty, fmt.Errorf("count native query: %w", err)
	}
	if response == nil {
		return empty, protocolError("Count", "response is empty")
	}
	result := CountResponse{Count: response.GetCount(), Estimated: response.GetEstimated()}
	return result, nil
}

func (p *Projection) toProto() (*sinkv1.Projection, error) {
	if p == nil {
		return nil, nil
	}
	seen := make(map[string]bool)
	for _, field := range p.Fields {
		if strings.TrimSpace(field) == "" || !utf8.ValidString(field) || seen[field] {
			return nil, errors.New("projection fields must be nonempty, valid UTF-8 and unique")
		}
		seen[field] = true
	}
	projection := &sinkv1.Projection{Fields: append([]string(nil), p.Fields...), Exclude: p.Exclude}
	return projection, nil
}
