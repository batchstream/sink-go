package sink

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"reflect"
	"sort"
	"strings"

	sinkv1 "github.com/liran/sink-go/api/sink/v1"
	"github.com/liran/sink-go/uri"
	"go.mongodb.org/mongo-driver/v2/bson"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Command is shared by Execute, Query, Count and Scan. Store configuration selects the
// adapter; use only the fields that adapter needs. Payload contains native
// command or body bytes, never an additional Sink-specific envelope.
// URI identifies a resource; Path describes an operation relative to it.
type Command struct {
	URI         string
	Method      string
	Path        string
	Query       string
	Headers     http.Header
	ContentType string
	Payload     []byte
}

// NewBSONCommand encodes an ordered document without opening a database
// connection. Use a struct, bson.D, or bson.Raw to preserve command field order.
// The Store interprets the target resource URI and command semantics.
func NewBSONCommand(target string, value any) (Command, error) {
	var empty Command
	address, err := uri.Parse(target)
	if err != nil || value == nil {
		return empty, errors.New("BSON command requires a canonical resource URI and ordered value")
	}
	valueType := reflect.TypeOf(value)
	for valueType.Kind() == reflect.Pointer {
		valueType = valueType.Elem()
	}
	if valueType.Kind() == reflect.Map {
		return empty, errors.New("BSON commands must preserve field order; use a struct, bson.D, or bson.Raw")
	}
	payload, err := bson.Marshal(value)
	if err != nil {
		return empty, fmt.Errorf("encode BSON command: %w", err)
	}
	command := Command{URI: address.String(), ContentType: "application/bson", Payload: payload}
	return command, nil
}

type ExecuteRequest struct {
	Command Command
}

type ExecuteResponse struct {
	ContentType string
	Payload     []byte
	Success     bool
	StatusCode  int
	Headers     http.Header
}

// NativeError retains the database's complete response, including errors not
// represented by the record API's storage-independent failure codes.
type NativeError struct {
	Response ExecuteResponse
}

func (e *NativeError) Error() string {
	if e.Response.StatusCode != 0 {
		return fmt.Sprintf("native database command returned HTTP %d", e.Response.StatusCode)
	}
	return "native database command failed; inspect its native response"
}

// Decode interprets a native JSON or BSON response. Payload remains available
// for responses in another content type and for byte-preserving forwarding.
func (r ExecuteResponse) Decode(destination any) error {
	contentType, _, err := mime.ParseMediaType(r.ContentType)
	if err != nil {
		return fmt.Errorf("decode native response content type: %w", err)
	}
	if contentType == "application/bson" {
		return bson.Unmarshal(r.Payload, destination)
	}
	if contentType == "application/json" || strings.HasSuffix(contentType, "+json") {
		return json.Unmarshal(r.Payload, destination)
	}
	return fmt.Errorf("native content type %q requires decoding Payload directly", r.ContentType)
}

func (c Command) toProto() (*sinkv1.Command, error) {
	if _, err := uri.Parse(c.URI); err != nil {
		return nil, fmt.Errorf("native command URI: %w", err)
	}
	if len(c.Payload) > 0 && c.ContentType == "" {
		return nil, errors.New("native payload requires ContentType")
	}
	if c.ContentType != "" {
		mediaType, _, err := mime.ParseMediaType(c.ContentType)
		if err != nil {
			return nil, fmt.Errorf("invalid native ContentType: %w", err)
		}
		if mediaType == "application/bson" && len(c.Payload) > 0 {
			if err := bson.Raw(c.Payload).Validate(); err != nil {
				return nil, fmt.Errorf("invalid BSON command: %w", err)
			}
		}
	}
	command := &sinkv1.Command{Uri: c.URI, Method: c.Method, Path: c.Path,
		Query: c.Query, ContentType: c.ContentType, Payload: bytes.Clone(c.Payload)}
	names := make([]string, 0, len(c.Headers))
	for name, values := range c.Headers {
		if name == "" || len(values) == 0 {
			return nil, errors.New("native header requires a name and values")
		}
		if strings.EqualFold(name, "Content-Type") {
			return nil, errors.New("set ContentType directly, not in Headers")
		}
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		header := &sinkv1.Header{Name: name, Values: append([]string(nil), c.Headers[name]...)}
		command.Headers = append(command.Headers, header)
	}
	return command, nil
}

// Execute makes one native command request. The SDK never retries it.
// MongoDB cursor and session commands are rejected; use Scan for resumable find
// pages or Query for independent find/aggregate pages.
// The server validates supported commands and rejects search index lifecycle
// and alias management. Permitted native writes follow adapter safeguards.
// A database error returns both its response and a *NativeError. Transport
// failures return no database response and must not be assumed unapplied.
func (c *Client) Execute(ctx context.Context, req ExecuteRequest) (ExecuteResponse, error) {
	var empty ExecuteResponse
	if c == nil || c.rpc == nil {
		return empty, errors.New("execute native command: client is required")
	}
	command, err := req.Command.toProto()
	if err != nil {
		return empty, err
	}
	request := &sinkv1.ExecuteRequest{Command: command}
	response, err := c.rpc.Execute(ctx, request, c.config.sinkCallOptions...)
	if err != nil {
		return empty, fmt.Errorf("execute native command: %w", err)
	}
	if response == nil {
		return empty, protocolError("Execute", "response is empty")
	}
	headers := make(http.Header)
	for _, header := range response.GetHeaders() {
		for _, value := range header.GetValues() {
			headers.Add(header.GetName(), value)
		}
	}
	result := ExecuteResponse{ContentType: response.GetContentType(), Payload: bytes.Clone(response.GetPayload()),
		Success: response.GetSuccess(), StatusCode: int(response.GetStatusCode()), Headers: headers}
	if !result.Success {
		failure := &NativeError{Response: result}
		return result, failure
	}
	return result, nil
}

type ScanRequest struct {
	Command    Command
	BatchSize  int // Zero selects 100; maximum 1000.
	Cursor     []byte
	Projection *Projection // Nil preserves the native projection.
	// OnDocument consumes each document without collecting it in the response.
	// Nil collects documents. Returning an error cancels the stream.
	OnDocument DocumentCallback
}

type ScanResponse struct {
	Documents  []Document
	NextCursor []byte
}

// Scan returns one live page without retaining a server session. Reuse Command
// and Projection, and pass NextCursor back after processing Documents. An empty
// NextCursor marks the end observed by this request. Cursors do not expire and
// survive server restarts. Concurrent changes can affect pages and retries.
// The SDK retries only explicitly marked temporary admission rejections, using
// the same request and cursor within ScanRetry and ScanTimeout. Checkpointing
// and idempotent processing belong to the caller. Cancellation of one request
// does not invalidate an existing cursor.
// An optional callback receives documents without collecting them in Documents.
// NextCursor is returned only after the stream finishes successfully.
func (c *Client) Scan(ctx context.Context, req ScanRequest) (ScanResponse, error) {
	var empty ScanResponse
	if c == nil || c.rpc == nil {
		return empty, errors.New("scan requires a client")
	}
	if req.BatchSize == 0 {
		req.BatchSize = defaultPageSize
	}
	if req.BatchSize < 0 || req.BatchSize > 1000 {
		return empty, errors.New("scan batch size must be between 0 and 1000")
	}
	if len(req.Cursor) > 64<<10 {
		return empty, errors.New("scan cursor exceeds byte limit")
	}
	command, err := req.Command.toProto()
	if err != nil {
		return empty, err
	}
	request := &sinkv1.ScanRequest{Command: command, BatchSize: uint32(req.BatchSize), Cursor: bytes.Clone(req.Cursor)}
	request.Projection, err = req.Projection.toProto()
	if err != nil {
		return empty, err
	}
	if c.config.scanTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.config.scanTimeout)
		defer cancel()
	}
	limit := req.BatchSize
	result := ScanResponse{}
	backoff := c.config.scanRetry.InitialBackoff
	for attempt := 1; attempt <= c.config.scanRetry.MaxAttempts; attempt++ {
		received := 0
		call := scanPageCall{request: request, limit: limit}
		call.emit = func(document Document) error {
			received++
			if req.OnDocument != nil {
				return req.OnDocument(document)
			}
			result.Documents = append(result.Documents, document)
			return nil
		}
		cursor, err := c.scanPage(ctx, call)
		if err == nil {
			result.NextCursor = cursor
			return result, nil
		}
		if received > 0 || attempt == c.config.scanRetry.MaxAttempts || !retryableScanAdmission(err) {
			return result, fmt.Errorf("scan page: %w", err)
		}
		if err := waitForBackoff(ctx, jitteredBackoff(backoff, c.config.scanRetry.Jitter)); err != nil {
			return result, fmt.Errorf("scan page: %w", status.FromContextError(err).Err())
		}
		backoff = nextBackoff(backoff, c.config.scanRetry)
	}
	return result, nil
}

type scanPageCall struct {
	request *sinkv1.ScanRequest
	limit   int
	emit    DocumentCallback
}

func (c *Client) scanPage(ctx context.Context, call scanPageCall) ([]byte, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	stream, err := c.rpc.Scan(ctx, call.request, c.config.sinkCallOptions...)
	if err != nil {
		return nil, err
	}
	var cursor []byte
	complete, count := false, 0
	for {
		frame, err := stream.Recv()
		if err == io.EOF {
			if !complete {
				return nil, protocolError("Scan", "stream omitted completion")
			}
			return cursor, nil
		}
		if err != nil {
			return nil, err
		}
		if frame == nil || complete {
			return nil, protocolError("Scan", "unexpected frame after completion")
		}
		if frame.GetComplete() {
			if len(frame.Documents) != 0 || len(frame.NextCursor) > 64<<10 || (len(frame.NextCursor) > 0 && count == 0) {
				return nil, protocolError("Scan", "invalid page completion")
			}
			complete, cursor = true, bytes.Clone(frame.NextCursor)
			continue
		}
		if len(frame.Documents) != 1 || len(frame.NextCursor) != 0 || count >= call.limit {
			return nil, protocolError("Scan", "invalid document frame")
		}
		document, err := documentFromProto(frame.Documents[0])
		if err != nil {
			return nil, protocolError("Scan", err.Error())
		}
		count++
		if err := call.emit(document); err != nil {
			return nil, err
		}
	}
}

func retryableScanAdmission(err error) bool {
	failure := status.Convert(err)
	if failure.Code() != codes.ResourceExhausted {
		return false
	}
	for _, detail := range failure.Details() {
		info, ok := detail.(*errdetails.ErrorInfo)
		if ok && info.GetDomain() == "sink" && info.GetReason() == "SCAN_ADMISSION_REJECTED" {
			return true
		}
	}
	return false
}
