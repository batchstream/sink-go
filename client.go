package sink

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"sort"
	"strings"
	"time"

	sinkv1 "github.com/batchstream/sink-go/api/sink/v1"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/balancer/roundrobin"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	healthv1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/status"
)

const (
	defaultMaxOperations      = 1000
	defaultReadAttempts       = 3
	defaultReadBackoff        = 100 * time.Millisecond
	defaultMaxBackoff         = time.Second
	defaultMultiplier         = 2
	defaultRetryJitter        = 0.2
	defaultMaxMessageBytes    = 64 << 20
	defaultDNSRefreshInterval = 30 * time.Second
)

// RetryPolicy controls backoff and attempt limits. Read retries Unavailable and
// retryable per-operation failures; Scan retries only explicit temporary
// admission rejections. Mutating RPCs are never retried automatically.
type RetryPolicy struct {
	MaxAttempts    int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	Multiplier     float64
	Jitter         float64
}

// ClientOptions controls request limits and safe read retries.
type ClientOptions struct {
	MaxOperations int
	ReadRetry     RetryPolicy
	ScanRetry     RetryPolicy

	// ScanTimeout bounds a whole page, including admission retries and backoff.
	// Zero adds no deadline. A positive value is an explicit caller-selected
	// page limit; a shorter context deadline wins.
	ScanTimeout            time.Duration
	MaxReceiveMessageBytes int
	MaxSendMessageBytes    int
}

// DialOptions controls connection creation. TLS with a minimum version of 1.2
// is used when TransportCredentials is nil. Use insecure.NewCredentials only
// for a trusted plaintext development endpoint.
type DialOptions struct {
	Client               ClientOptions
	TransportCredentials credentials.TransportCredentials
	GRPCOptions          []grpc.DialOption

	// DNSRefreshInterval is the delay after a successful DNS update before
	// another lookup. Zero defaults to 30 seconds; negative values are invalid.
	// This is per client and does not bypass DNS-server caches. Explicit
	// resolvers in GRPCOptions control their own refresh behavior.
	DNSRefreshInterval time.Duration
}

type clientConfig struct {
	maxOperations     int
	readRetry         RetryPolicy
	scanRetry         RetryPolicy
	scanTimeout       time.Duration
	sinkCallOptions   []grpc.CallOption
	healthCallOptions []grpc.CallOption
}

// Client is safe for concurrent use.
type Client struct {
	rpc        sinkv1.SinkClient
	health     healthv1.HealthClient
	connection *grpc.ClientConn
	config     clientConfig
}

// Dial creates a lazily connected gRPC client with round-robin balancing across
// resolved addresses. Use a DNS target exposing backend addresses (for example
// a Kubernetes headless service) to distribute RPCs across replicas. Explicit
// GRPCOptions or resolver service configuration may override the default policy.
// DNS targets refresh at DNSRefreshInterval while the connection is active.
// Call CheckHealth when startup must prove the endpoint is reachable.
func Dial(target string, opts DialOptions) (*Client, error) {
	if strings.TrimSpace(target) == "" {
		return nil, errors.New("create Sink client: target is required")
	}
	if opts.DNSRefreshInterval < 0 {
		return nil, errors.New("create Sink client: DNS refresh interval cannot be negative")
	}
	dnsRefreshInterval := opts.DNSRefreshInterval
	if dnsRefreshInterval == 0 {
		dnsRefreshInterval = defaultDNSRefreshInterval
	}
	transportCredentials := opts.TransportCredentials
	if transportCredentials == nil {
		tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
		transportCredentials = credentials.NewTLS(tlsConfig)
	}
	balancing := grpc.WithDefaultServiceConfig(`{"loadBalancingConfig":[{"round_robin":{}}]}`)
	grpcOptions := []grpc.DialOption{balancing}
	grpcOptions = append(grpcOptions, opts.GRPCOptions...)
	// gRPC selects the first matching resolver, so caller-provided resolvers
	// retain precedence over the periodically refreshed DNS default.
	dnsBuilder := &refreshingDNSBuilder{Builder: resolver.Get("dns"), interval: dnsRefreshInterval}
	grpcOptions = append(grpcOptions, grpc.WithResolvers(dnsBuilder))
	transportOption := grpc.WithTransportCredentials(transportCredentials)
	grpcOptions = append(grpcOptions, transportOption)
	connection, err := grpc.NewClient(target, grpcOptions...)
	if err != nil {
		return nil, fmt.Errorf("create Sink connection: %w", err)
	}
	client, err := New(connection, opts.Client)
	if err != nil {
		_ = connection.Close()
		return nil, err
	}
	client.connection = connection
	return client, nil
}

// New wraps an existing connection without taking ownership of it.
func New(connection grpc.ClientConnInterface, opts ClientOptions) (*Client, error) {
	if connection == nil {
		return nil, errors.New("create Sink client: connection is required")
	}
	config, err := newClientConfig(opts)
	if err != nil {
		return nil, err
	}
	client := &Client{
		rpc:    sinkv1.NewSinkClient(connection),
		health: healthv1.NewHealthClient(connection),
		config: config,
	}
	return client, nil
}

func newClientConfig(opts ClientOptions) (clientConfig, error) {
	var config clientConfig
	if opts.MaxOperations < 0 || opts.MaxReceiveMessageBytes < 0 || opts.MaxSendMessageBytes < 0 || opts.ScanTimeout < 0 {
		return config, errors.New("create Sink client: limits cannot be negative")
	}
	maxOperations := opts.MaxOperations
	if maxOperations == 0 {
		maxOperations = defaultMaxOperations
	}
	retry, err := normalizeRetryPolicy(opts.ReadRetry)
	if err != nil {
		return config, fmt.Errorf("create Sink client: read retry: %w", err)
	}
	scanRetry, err := normalizeRetryPolicy(opts.ScanRetry)
	if err != nil {
		return config, fmt.Errorf("create Sink client: scan retry: %w", err)
	}
	scanTimeout := opts.ScanTimeout
	maxReceiveBytes := opts.MaxReceiveMessageBytes
	if maxReceiveBytes == 0 {
		maxReceiveBytes = defaultMaxMessageBytes
	}
	maxSendBytes := opts.MaxSendMessageBytes
	if maxSendBytes == 0 {
		maxSendBytes = defaultMaxMessageBytes
	}
	healthCallOptions := []grpc.CallOption{
		grpc.MaxCallRecvMsgSize(maxReceiveBytes),
		grpc.MaxCallSendMsgSize(maxSendBytes),
	}
	sinkCallOptions := append([]grpc.CallOption(nil), healthCallOptions...)
	vtCodec := newVTProtoCodec()
	sinkCallOptions = append(sinkCallOptions, grpc.ForceCodecV2(vtCodec))
	config = clientConfig{
		maxOperations:     maxOperations,
		readRetry:         retry,
		scanRetry:         scanRetry,
		scanTimeout:       scanTimeout,
		sinkCallOptions:   sinkCallOptions,
		healthCallOptions: healthCallOptions,
	}
	return config, nil
}

func normalizeRetryPolicy(policy RetryPolicy) (RetryPolicy, error) {
	var empty RetryPolicy
	if policy.MaxAttempts < 0 {
		return empty, errors.New("max attempts cannot be negative")
	}
	if policy.InitialBackoff < 0 || policy.MaxBackoff < 0 {
		return empty, errors.New("backoff cannot be negative")
	}
	if policy.Multiplier < 0 {
		return empty, errors.New("multiplier cannot be negative")
	}
	if policy.Jitter < 0 || policy.Jitter > 1 {
		return empty, errors.New("jitter must be between 0 and 1")
	}
	if policy.MaxAttempts == 0 {
		policy.MaxAttempts = defaultReadAttempts
	}
	if policy.InitialBackoff == 0 {
		policy.InitialBackoff = defaultReadBackoff
	}
	if policy.MaxBackoff == 0 {
		policy.MaxBackoff = defaultMaxBackoff
	}
	if policy.Multiplier == 0 {
		policy.Multiplier = defaultMultiplier
	}
	if policy.Jitter == 0 {
		policy.Jitter = defaultRetryJitter
	}
	if policy.MaxBackoff < policy.InitialBackoff {
		return empty, errors.New("max backoff is less than initial backoff")
	}
	if policy.Multiplier < 1 {
		return empty, errors.New("multiplier must be at least 1")
	}
	return policy, nil
}

// Close closes connections created by Dial. It is a no-op for clients created
// with New because the caller owns that connection.
func (c *Client) Close() error {
	if c == nil || c.connection == nil {
		return nil
	}
	return c.connection.Close()
}

// Raw returns the generated gRPC client for advanced or forward-compatible
// usage.
func (c *Client) Raw() sinkv1.SinkClient {
	if c == nil {
		return nil
	}
	return c.rpc
}

// CheckHealth verifies that the server's standard gRPC health service reports
// SERVING.
func (c *Client) CheckHealth(ctx context.Context) error {
	if c == nil || c.health == nil {
		return errors.New("check Sink health: client is nil")
	}
	request := &healthv1.HealthCheckRequest{}
	response, err := c.health.Check(ctx, request, c.config.healthCallOptions...)
	if err != nil {
		return fmt.Errorf("check Sink health: %w", err)
	}
	if response == nil {
		return errors.New("check Sink health: server returned an empty response")
	}
	if response.GetStatus() != healthv1.HealthCheckResponse_SERVING {
		return fmt.Errorf("check Sink health: server status is %s", response.GetStatus())
	}
	return nil
}

// Read collects results in request order, or passes them in arrival order to an
// optional callback without collecting them. It automatically splits large
// collections into configured operation-count batches. Transport-level
// Unavailable errors and retryable per-operation failures are retried because
// reads are idempotent. Returned operation indexes refer to the original
// collection.
func (c *Client) Read(ctx context.Context, req ReadRequest) ([]ReadResult, error) {
	addresses := req.Addresses
	if err := c.validateCollection("read", len(addresses)); err != nil {
		return nil, err
	}
	operations := make([]*sinkv1.ReadOperation, len(addresses))
	for index, address := range addresses {
		if err := address.validate(); err != nil {
			return nil, fmt.Errorf("read operation %d: %w", index, err)
		}
		protoAddress := address.toProto()
		operation := &sinkv1.ReadOperation{Address: protoAddress}
		operations[index] = operation
	}
	var results []ReadResult
	defer func() {
		sort.Slice(results, func(i, j int) bool { return results[i].OperationIndex < results[j].OperationIndex })
	}()
	for start := 0; start < len(operations); start += c.config.maxOperations {
		end := min(start+c.config.maxOperations, len(operations))
		err := c.readOperationsWithRetry(ctx, operations[start:end], func(result ReadResult) error {
			result.OperationIndex += start
			if result.Failure != nil {
				result.Failure.OperationIndex += start
			}
			if req.OnResult != nil {
				return req.OnResult(result)
			}
			results = append(results, result)
			return nil
		})
		if err != nil {
			return results, fmt.Errorf("read records: %w", err)
		}
	}
	return results, nil
}

// Write submits mixed put and merge operations and automatically splits large
// collections into configured operation-count batches. It deliberately does
// not retry transport failures because the server may already have applied or
// durably accepted the mutation. Earlier batches may have completed when a
// later batch returns an error. Without a callback, acknowledged results are
// collected in request order. With a callback, results arrive incrementally
// and the returned result slice is nil.
func (c *Client) Write(ctx context.Context, req WriteRequest) ([]WriteResult, error) {
	completionMode, operations := defaultCompletionMode(req.CompletionMode), req.Operations
	if err := c.validateCollection("write", len(operations)); err != nil {
		return nil, err
	}
	if !validCompletionMode(completionMode) {
		return nil, errors.New("write request has an invalid completion mode")
	}
	for index, operation := range operations {
		if operation.returnDocument && completionMode == CompletionReturnAfterAccepted {
			return nil, errors.New("returned write documents require synchronous completion")
		}
		if err := operation.validate(); err != nil {
			return nil, fmt.Errorf("write operation %d: %w", index, err)
		}
	}
	var results []WriteResult
	defer func() {
		sort.Slice(results, func(i, j int) bool { return results[i].OperationIndex < results[j].OperationIndex })
	}()
	for start := 0; start < len(operations); start += c.config.maxOperations {
		end := min(start+c.config.maxOperations, len(operations))
		call := writeBatchCall{mode: completionMode, operations: operations[start:end]}
		call.emit = func(result WriteResult) error {
			result.OperationIndex += start
			if result.Failure != nil {
				result.Failure.OperationIndex += start
			}
			if req.OnResult != nil {
				return req.OnResult(result)
			}
			results = append(results, result)
			return nil
		}
		if err := c.writeBatch(ctx, call); err != nil {
			return results, err
		}
	}
	return results, nil
}

type writeBatchCall struct {
	mode       CompletionMode
	operations []WriteOperation
	emit       WriteCallback
}

func (c *Client) writeBatch(ctx context.Context, call writeBatchCall) error {
	operations, completionMode := call.operations, call.mode

	protoOperations := make([]*sinkv1.WriteOperation, len(operations))
	luaPrograms := make([]*sinkv1.LuaProgram, 0)
	seenLuaPrograms := make(map[[sha256.Size]byte]struct{})
	for index, operation := range operations {
		protoOperations[index] = operation.toProto()
		if operation.action == writeActionMerge {
			digest := operation.merge.program.sha256
			if _, exists := seenLuaPrograms[digest]; !exists {
				program := &sinkv1.LuaProgram{
					Source: operation.merge.program.Source(),
					Sha256: operation.merge.program.SHA256(),
				}
				luaPrograms = append(luaPrograms, program)
				seenLuaPrograms[digest] = struct{}{}
			}
		}
	}
	request := &sinkv1.WriteRequest{
		CompletionMode: completionMode,
		Operations:     protoOperations,
		LuaPrograms:    luaPrograms,
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := c.rpc.Write(ctx, request, c.config.sinkCallOptions...)
	if err != nil {
		return fmt.Errorf("write records: %w", err)
	}
	seen := make([]bool, len(operations))
	count := 0
	for {
		frame, err := stream.Recv()
		if err == io.EOF {
			if count != len(operations) {
				return protocolError("Write", "stream omitted operation results")
			}
			return nil
		}
		if err != nil {
			return fmt.Errorf("write records: %w", err)
		}
		if frame == nil || len(frame.GetResults()) != 1 || frame.Results[0] == nil {
			return protocolError("Write", "expected one result per frame")
		}
		raw := frame.Results[0]
		index, err := validateResultIndex("Write", raw.GetOperationIndex(), seen)
		if err != nil {
			return err
		}
		result, err := decodeWriteResult(raw, index)
		if err != nil {
			return err
		}
		if operations[index].returnDocument && result.Status == WriteApplied && len(result.Document.payload) == 0 {
			return protocolError("Write", "applied operation omitted the requested document")
		}
		if err := call.emit(result); err != nil {
			return err
		}
		count++
	}
}

// Delete permanently deletes records. Deleting an absent record is successful.
// Large collections are split automatically, and transport failures are not
// retried. Earlier batches may have completed when a later batch returns an
// error.
func (c *Client) Delete(ctx context.Context, req DeleteRequest) ([]DeleteResult, error) {
	completionMode, addresses := defaultCompletionMode(req.CompletionMode), req.Addresses
	if err := c.validateCollection("delete", len(addresses)); err != nil {
		return nil, err
	}
	if !validCompletionMode(completionMode) {
		return nil, errors.New("delete request has an invalid completion mode")
	}
	operations := make([]*sinkv1.DeleteOperation, len(addresses))
	for index, address := range addresses {
		if err := address.validate(); err != nil {
			return nil, fmt.Errorf("delete operation %d: %w", index, err)
		}
		protoAddress := address.toProto()
		operation := &sinkv1.DeleteOperation{Address: protoAddress}
		operations[index] = operation
	}
	results := make([]DeleteResult, 0, len(addresses))
	for start := 0; start < len(operations); start += c.config.maxOperations {
		end := min(start+c.config.maxOperations, len(operations))
		batchOperations := operations[start:end]
		request := &sinkv1.DeleteRequest{
			CompletionMode: completionMode,
			Operations:     batchOperations,
		}
		response, err := c.rpc.Delete(ctx, request, c.config.sinkCallOptions...)
		if err != nil {
			return results, fmt.Errorf("delete records: %w", err)
		}
		batch, err := decodeDeleteResponse(response, len(batchOperations))
		if err != nil {
			return results, err
		}
		remapDeleteIndexes(batch, start)
		results = append(results, batch...)
	}
	return results, nil
}

func (c *Client) validateCollection(method string, count int) error {
	if c == nil || c.rpc == nil {
		return fmt.Errorf("%s records: client is nil", method)
	}
	if count == 0 {
		return fmt.Errorf("%s request must contain operations", method)
	}
	return nil
}

func remapDeleteIndexes(results []DeleteResult, offset int) {
	for index := range results {
		results[index].OperationIndex += offset
		if results[index].Failure != nil {
			results[index].Failure.OperationIndex += offset
		}
	}
}

func validCompletionMode(mode CompletionMode) bool {
	return mode == CompletionWaitUntilApplied || mode == CompletionReturnAfterAccepted ||
		mode == CompletionWaitUntilVisible
}

type pendingRead struct {
	index     int
	operation *sinkv1.ReadOperation
}

func (c *Client) readOperationsWithRetry(ctx context.Context, operations []*sinkv1.ReadOperation, emit ReadCallback) error {
	pending := make([]pendingRead, len(operations))
	for index, operation := range operations {
		pending[index] = pendingRead{index: index, operation: operation}
	}
	backoff := c.config.readRetry.InitialBackoff
	for attempt := 1; attempt <= c.config.readRetry.MaxAttempts; attempt++ {
		request := &sinkv1.ReadRequest{Operations: make([]*sinkv1.ReadOperation, len(pending))}
		for index, work := range pending {
			request.Operations[index] = work.operation
		}
		execution, cancel := context.WithCancel(ctx)
		stream, transportErr := c.rpc.Read(execution, request, c.config.sinkCallOptions...)
		seen := make([]bool, len(pending))
		next := make([]pendingRead, 0)
		count := 0
		if transportErr == nil {
			for {
				frame, err := stream.Recv()
				if err == io.EOF {
					if count != len(pending) {
						cancel()
						return protocolError("Read", "stream omitted operation results")
					}
					break
				}
				if err != nil {
					transportErr = err
					break
				}
				if frame == nil || len(frame.GetResults()) != 1 || frame.Results[0] == nil {
					cancel()
					return protocolError("Read", "expected one result per frame")
				}
				raw := frame.Results[0]
				index, err := validateResultIndex("Read", raw.GetOperationIndex(), seen)
				if err != nil {
					cancel()
					return err
				}
				work := pending[index]
				result, err := decodeReadResult(raw, work.index)
				if err != nil {
					cancel()
					return err
				}
				count++
				if attempt < c.config.readRetry.MaxAttempts && result.Failure != nil && result.Failure.Retryable {
					next = append(next, work)
				} else if err := emit(result); err != nil {
					cancel()
					return err
				}
			}
		}
		cancel()
		if transportErr != nil {
			if attempt == c.config.readRetry.MaxAttempts || status.Code(transportErr) != codes.Unavailable {
				return transportErr
			}
			// Never redeliver a result already observed by the caller.
			for index, work := range pending {
				if !seen[index] {
					next = append(next, work)
				}
			}
		}
		if len(next) == 0 {
			return transportErr
		}
		if err := waitForBackoff(ctx, jitteredBackoff(backoff, c.config.readRetry.Jitter)); err != nil {
			return err
		}
		pending = next
		backoff = nextBackoff(backoff, c.config.readRetry)
	}
	return nil
}

func waitForBackoff(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	select {
	case <-ctx.Done():
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func nextBackoff(current time.Duration, policy RetryPolicy) time.Duration {
	if current >= policy.MaxBackoff {
		return policy.MaxBackoff
	}
	maximumMultiplier := float64(policy.MaxBackoff) / float64(current)
	if policy.Multiplier >= maximumMultiplier {
		return policy.MaxBackoff
	}
	next := time.Duration(float64(current) * policy.Multiplier)
	return next
}

func jitteredBackoff(backoff time.Duration, jitter float64) time.Duration {
	if jitter <= 0 || backoff <= 1 {
		return backoff
	}
	minimum := float64(backoff) * (1 - jitter)
	spread := float64(backoff) * jitter * 2
	return time.Duration(minimum + rand.Float64()*spread)
}
