package sink

import (
	"context"
	"errors"
	"fmt"

	"github.com/batchstream/sink-protocol/uri"
)

// DatasetOptions binds the stable routing, encoding, and optional merge
// program shared by records in one logical dataset. Completion mode remains a
// per-call choice because callers of the same dataset can require different
// durability and visibility guarantees.
type DatasetOptions struct {
	URI          string
	Encoding     DocumentEncoding
	MergeProgram *LuaProgram
}

// Record pairs one logical key with the Go value to encode for a Dataset
// mutation.
type Record struct {
	Key   Key
	Value any
	// ReturnDocument requires a synchronous completion mode.
	ReturnDocument bool
}

// Dataset binds record and native operations to one routing and encoding scope.
// Record methods return decoded results and a BatchError when any individual
// operation fails; native methods retain the corresponding Client semantics.
type Dataset struct {
	client       *Client
	uri          string
	encoding     DocumentEncoding
	mergeProgram *LuaProgram
}

// NewDataset binds stable dataset settings to a Client. MergeProgram is
// optional and is copied when configured.
func NewDataset(client *Client, opts DatasetOptions) (*Dataset, error) {
	if client == nil || client.rpc == nil {
		return nil, errors.New("create dataset: client is required")
	}
	if _, err := uri.Parse(opts.URI); err != nil {
		return nil, fmt.Errorf("create dataset: %w", err)
	}
	if opts.Encoding != DocumentEncodingJSON && opts.Encoding != DocumentEncodingBSON {
		return nil, errors.New("create dataset: document encoding is required")
	}
	dataset := &Dataset{
		client:   client,
		uri:      opts.URI,
		encoding: opts.Encoding,
	}
	if opts.MergeProgram != nil {
		if err := opts.MergeProgram.validate(); err != nil {
			return nil, fmt.Errorf("create dataset: %w", err)
		}
		// Snapshot the value; LuaProgram owns immutable buffers.
		program := *opts.MergeProgram
		dataset.mergeProgram = &program
	}
	return dataset, nil
}

// Read fetches one or more records by key, splits large collections automatically,
// and treats not-found results as successful reads. Without a callback, results
// are collected in key order. With a callback, results arrive incrementally
// and the returned result slice is nil.
func (d *Dataset) Read(ctx context.Context, req DatasetReadRequest) ([]ReadResult, error) {
	keys := req.Keys
	if err := d.validate("read"); err != nil {
		return nil, err
	}
	addresses := make([]Address, len(keys))
	for index, key := range keys {
		address, err := NewRecordAddress(d.uri, key)
		if err != nil {
			return nil, fmt.Errorf("dataset read key %d: %w", index, err)
		}
		addresses[index] = address
	}
	callback := req.OnResult
	var failures []*OperationError
	var onResult ReadCallback
	if callback != nil {
		onResult = func(result ReadResult) error {
			if result.Failure != nil {
				failures = append(failures, result.Failure)
			}
			return callback(result)
		}
	}
	request := ReadRequest{Addresses: addresses, OnResult: onResult}
	results, err := d.client.Read(ctx, request)
	err = errors.Join(err, ReadResultsError(results), newBatchError(failures))
	if err != nil {
		return results, fmt.Errorf("dataset read: %w", err)
	}
	return results, nil
}

// Create writes complete documents only when their keys do not already exist.
func (d *Dataset) Create(ctx context.Context, req DatasetWriteRequest) ([]WriteResult, error) {
	opts := datasetPutOptions{
		request:   req,
		writeMode: WriteCreate,
		operation: "create",
	}
	return d.put(ctx, opts)
}

// Replace writes complete documents only when their keys already exist.
func (d *Dataset) Replace(ctx context.Context, req DatasetWriteRequest) ([]WriteResult, error) {
	opts := datasetPutOptions{
		request:   req,
		writeMode: WriteReplace,
		operation: "replace",
	}
	return d.put(ctx, opts)
}

// Upsert writes complete documents whether or not their keys already exist.
func (d *Dataset) Upsert(ctx context.Context, req DatasetWriteRequest) ([]WriteResult, error) {
	opts := datasetPutOptions{
		request:   req,
		writeMode: WriteUpsert,
		operation: "upsert",
	}
	return d.put(ctx, opts)
}

// Merge atomically applies the Dataset's bound Lua program to every incoming
// record, creating a record when none exists. A Dataset without MergeProgram
// rejects Merge before sending an RPC.
func (d *Dataset) Merge(ctx context.Context, req DatasetWriteRequest) ([]WriteResult, error) {
	if err := d.validate("merge"); err != nil {
		return nil, err
	}
	if d.mergeProgram == nil {
		return nil, errors.New("dataset merge: merge program is not configured")
	}
	operations := make([]WriteOperation, len(req.Records))
	for index, record := range req.Records {
		address, document, err := d.encodeRecord(record)
		if err != nil {
			return nil, fmt.Errorf("dataset merge record %d: %w", index, err)
		}
		mergeOptions := MergeOptions{
			Incoming: document,
			Program:  *d.mergeProgram,
		}
		operation, err := NewMerge(address, mergeOptions)
		if err != nil {
			return nil, fmt.Errorf("dataset merge record %d: %w", index, err)
		}
		operation.returnDocument = record.ReturnDocument
		operations[index] = operation
	}
	request := WriteRequest{CompletionMode: req.CompletionMode, Operations: operations, OnResult: req.OnResult}
	return d.write(ctx, "merge", request)
}

type datasetPutOptions struct {
	request   DatasetWriteRequest
	writeMode WriteMode
	operation string
}

func (d *Dataset) put(ctx context.Context, opts datasetPutOptions) ([]WriteResult, error) {
	if err := d.validate(opts.operation); err != nil {
		return nil, err
	}
	operations := make([]WriteOperation, len(opts.request.Records))
	for index, record := range opts.request.Records {
		address, document, err := d.encodeRecord(record)
		if err != nil {
			return nil, fmt.Errorf("dataset %s record %d: %w", opts.operation, index, err)
		}
		operation, err := NewPut(address, document, opts.writeMode)
		if err != nil {
			return nil, fmt.Errorf("dataset %s record %d: %w", opts.operation, index, err)
		}
		operation.returnDocument = record.ReturnDocument
		operations[index] = operation
	}
	request := WriteRequest{CompletionMode: opts.request.CompletionMode, Operations: operations, OnResult: opts.request.OnResult}
	return d.write(ctx, opts.operation, request)
}

func (d *Dataset) validate(operation string) error {
	if d == nil || d.client == nil || d.client.rpc == nil {
		return fmt.Errorf("dataset %s: client is required", operation)
	}
	return nil
}

func (d *Dataset) encodeRecord(record Record) (Address, Document, error) {
	var emptyAddress Address
	var emptyDocument Document
	address, err := NewRecordAddress(d.uri, record.Key)
	if err != nil {
		return emptyAddress, emptyDocument, err
	}
	document, err := NewDocument(record.Value, d.encoding)
	if err != nil {
		return emptyAddress, emptyDocument, err
	}
	return address, document, nil
}

func (d *Dataset) write(ctx context.Context, operation string, req WriteRequest) ([]WriteResult, error) {
	callback := req.OnResult
	var failures []*OperationError
	var onResult WriteCallback
	if callback != nil {
		onResult = func(result WriteResult) error {
			if result.Failure != nil {
				failures = append(failures, result.Failure)
			}
			return callback(result)
		}
	}
	req.OnResult = onResult
	results, err := d.client.Write(ctx, req)
	err = errors.Join(err, WriteResultsError(results), newBatchError(failures))
	if err != nil {
		return results, fmt.Errorf("dataset %s: %w", operation, err)
	}
	return results, nil
}
