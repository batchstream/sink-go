package sink

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/batchstream/sink-go/uri"

	sinkv1 "github.com/batchstream/sink-go/api/sink/v1"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type CompletionMode = sinkv1.CompletionMode

const (
	CompletionWaitUntilApplied    CompletionMode = sinkv1.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED
	CompletionReturnAfterAccepted CompletionMode = sinkv1.CompletionMode_COMPLETION_MODE_RETURN_AFTER_ACCEPTED
	CompletionWaitUntilVisible    CompletionMode = sinkv1.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_VISIBLE
)

type WriteMode = sinkv1.WriteMode

const (
	WriteCreate  WriteMode = sinkv1.WriteMode_WRITE_MODE_CREATE
	WriteReplace WriteMode = sinkv1.WriteMode_WRITE_MODE_REPLACE
	WriteUpsert  WriteMode = sinkv1.WriteMode_WRITE_MODE_UPSERT
)

type DocumentEncoding = sinkv1.DocumentEncoding

const (
	DocumentEncodingJSON DocumentEncoding = sinkv1.DocumentEncoding_DOCUMENT_ENCODING_JSON
	DocumentEncodingBSON DocumentEncoding = sinkv1.DocumentEncoding_DOCUMENT_ENCODING_BSON
)

type ReadStatus = sinkv1.ReadStatus

const (
	ReadFound    ReadStatus = sinkv1.ReadStatus_READ_STATUS_FOUND
	ReadNotFound ReadStatus = sinkv1.ReadStatus_READ_STATUS_NOT_FOUND
	ReadFailed   ReadStatus = sinkv1.ReadStatus_READ_STATUS_FAILED
)

type WriteStatus = sinkv1.WriteStatus

const (
	WriteAccepted           WriteStatus = sinkv1.WriteStatus_WRITE_STATUS_ACCEPTED
	WriteApplied            WriteStatus = sinkv1.WriteStatus_WRITE_STATUS_APPLIED
	WritePreconditionFailed WriteStatus = sinkv1.WriteStatus_WRITE_STATUS_PRECONDITION_FAILED
	WriteFailed             WriteStatus = sinkv1.WriteStatus_WRITE_STATUS_FAILED
)

type DeleteStatus = sinkv1.DeleteStatus

const (
	DeleteAccepted DeleteStatus = sinkv1.DeleteStatus_DELETE_STATUS_ACCEPTED
	DeleteApplied  DeleteStatus = sinkv1.DeleteStatus_DELETE_STATUS_APPLIED
	DeleteFailed   DeleteStatus = sinkv1.DeleteStatus_DELETE_STATUS_FAILED
)

type FailureCode = sinkv1.FailureCode

const (
	FailureInvalidArgument    FailureCode = sinkv1.FailureCode_FAILURE_CODE_INVALID_ARGUMENT
	FailurePreconditionFailed FailureCode = sinkv1.FailureCode_FAILURE_CODE_PRECONDITION_FAILED
	FailureNotFound           FailureCode = sinkv1.FailureCode_FAILURE_CODE_NOT_FOUND
	FailureConflict           FailureCode = sinkv1.FailureCode_FAILURE_CODE_CONFLICT
	FailureResourceExhausted  FailureCode = sinkv1.FailureCode_FAILURE_CODE_RESOURCE_EXHAUSTED
	FailureUnavailable        FailureCode = sinkv1.FailureCode_FAILURE_CODE_UNAVAILABLE
	FailureDeadlineExceeded   FailureCode = sinkv1.FailureCode_FAILURE_CODE_DEADLINE_EXCEEDED
	FailureInternal           FailureCode = sinkv1.FailureCode_FAILURE_CODE_INTERNAL
)

type keyKind uint8

const (
	keyKindUnspecified keyKind = iota
	keyKindString
	keyKindInt64
	keyKindBytes
	keyKindOpaque
)

// Key is an immutable logical record key. Use one of the key constructors.
type Key struct {
	kind        keyKind
	stringValue string
	int64Value  int64
	bytesValue  []byte
	opaqueType  string
}

func StringKey(value string) Key {
	key := Key{kind: keyKindString, stringValue: value}
	return key
}

func Int64Key(value int64) Key {
	key := Key{kind: keyKindInt64, int64Value: value}
	return key
}

func BytesKey(value []byte) Key {
	key := Key{kind: keyKindBytes, bytesValue: bytes.Clone(value)}
	return key
}

func OpaqueKey(typeName string, value []byte) (Key, error) {
	var empty Key
	if typeName == "" {
		return empty, errors.New("opaque key type is required")
	}
	key := Key{
		kind:       keyKindOpaque,
		bytesValue: bytes.Clone(value),
		opaqueType: typeName,
	}
	return key, nil
}

func (k Key) validate() error {
	if k.kind == keyKindUnspecified {
		return errors.New("record key is required")
	}
	if k.kind == keyKindOpaque && k.opaqueType == "" {
		return errors.New("opaque key type is required")
	}
	return nil
}

// Address wraps a canonical Store URI. Store adapters interpret the path.
type Address struct{ value uri.Address }

func NewAddress(value string) (Address, error) {
	parsed, err := uri.Parse(value)
	if err == nil && len(parsed.Segments()) == 0 {
		err = errors.New("record URI requires a resource path")
	}
	address := Address{value: parsed}
	return address, err
}

// NewRecordAddress appends a typed key to a Store-owned resource URI.
func NewRecordAddress(resource string, key Key) (Address, error) {
	var empty Address
	if err := key.validate(); err != nil {
		return empty, err
	}
	parsed, err := uri.AppendKey(resource, key.uriKey())
	if err != nil {
		return empty, err
	}
	address := Address{value: parsed}
	return address, nil
}

func (a Address) Store() string   { return a.value.Store() }
func (a Address) URI() string     { return a.value.String() }
func (a Address) validate() error { _, err := NewAddress(a.URI()); return err }

func (k Key) uriKey() uri.Key {
	key := uri.Key{}
	switch k.kind {
	case keyKindString:
		key.Type = "string"
		key.Data = []byte(k.stringValue)
	case keyKindInt64:
		key.Type = "int64"
		key.Data = make([]byte, 8)
		binary.BigEndian.PutUint64(key.Data, uint64(k.int64Value))
	case keyKindBytes:
		key.Type = "bytes"
		key.Data = bytes.Clone(k.bytesValue)
	case keyKindOpaque:
		key.Type = "opaque:" + k.opaqueType
		key.Data = bytes.Clone(k.bytesValue)
	}
	return key
}

// Document contains one immutable, explicitly encoded user object.
type Document struct {
	encoding DocumentEncoding
	payload  []byte
}

// NewDocument encodes a Go value using exactly the requested format. JSON uses
// json struct tags and BSON uses bson struct tags.
func NewDocument(value any, encoding DocumentEncoding) (Document, error) {
	var document Document
	var encoded []byte
	var err error
	switch encoding {
	case DocumentEncodingJSON:
		encoded, err = json.Marshal(value)
		if err != nil {
			return document, fmt.Errorf("encode JSON document: %w", err)
		}
	case DocumentEncodingBSON:
		encoded, err = bson.Marshal(value)
		if err != nil {
			return document, fmt.Errorf("encode BSON document: %w", err)
		}
	default:
		return document, errors.New("document encoding is required")
	}
	return NewRawDocument(encoding, encoded)
}

// NewRawDocument validates and copies an already encoded document payload.
func NewRawDocument(encoding DocumentEncoding, payload []byte) (Document, error) {
	document := Document{encoding: encoding, payload: bytes.Clone(payload)}
	if err := document.validate(); err != nil {
		var empty Document
		return empty, err
	}
	return document, nil
}

func (d Document) Encoding() DocumentEncoding {
	return d.encoding
}

func (d Document) Payload() []byte {
	return bytes.Clone(d.payload)
}

func (d Document) Decode(destination any) error {
	if destination == nil {
		return errors.New("decode document: destination is required")
	}
	var err error
	switch d.encoding {
	case DocumentEncodingJSON:
		err = json.Unmarshal(d.payload, destination)
	case DocumentEncodingBSON:
		err = bson.Unmarshal(d.payload, destination)
	default:
		return errors.New("decode document: encoding is required")
	}
	if err != nil {
		return fmt.Errorf("decode document: %w", err)
	}
	return nil
}

func (d Document) validate() error {
	switch d.encoding {
	case DocumentEncodingJSON:
		trimmed := bytes.TrimSpace(d.payload)
		if len(trimmed) < 2 || trimmed[0] != '{' || trimmed[len(trimmed)-1] != '}' || !json.Valid(trimmed) {
			return errors.New("document payload must contain a valid JSON object")
		}
	case DocumentEncodingBSON:
		raw := bson.Raw(d.payload)
		if err := raw.Validate(); err != nil {
			return fmt.Errorf("document payload must contain a valid BSON document: %w", err)
		}
	default:
		return errors.New("document encoding is required")
	}
	return nil
}

// LuaProgram is an immutable, self-contained merge rule. Sink verifies the
// digest and caches the compiled program while executing each merge in a fresh
// VM.
type LuaProgram struct {
	source []byte
	sha256 [sha256.Size]byte
}

func NewLuaProgram(source []byte) (LuaProgram, error) {
	var program LuaProgram
	if len(source) == 0 {
		return program, errors.New("lua merge program source is required")
	}
	program.source = bytes.Clone(source)
	program.sha256 = sha256.Sum256(source)
	return program, nil
}

func (p LuaProgram) Source() []byte {
	return bytes.Clone(p.source)
}

func (p LuaProgram) SHA256() []byte {
	digest := make([]byte, sha256.Size)
	copy(digest, p.sha256[:])
	return digest
}

func (p LuaProgram) validate() error {
	if len(p.source) == 0 {
		return errors.New("lua merge program source is required")
	}
	if sha256.Sum256(p.source) != p.sha256 {
		return errors.New("lua merge program source was modified")
	}
	return nil
}

type writeAction uint8

const (
	writeActionUnspecified writeAction = iota
	writeActionPut
	writeActionMerge
)

// MergeOptions describes a Lua-driven read-modify-write operation.
type MergeOptions struct {
	Incoming Document
	Program  LuaProgram
}

type mergeOperation struct {
	incoming Document
	program  LuaProgram
}

// WriteOperation is either a put or merge operation. Use NewPut or NewMerge.
type WriteOperation struct {
	returnDocument bool
	address        Address
	action         writeAction
	put            Document
	mode           WriteMode
	merge          mergeOperation
}

// WithReturnedDocument requests the logical document used by this operation's
// successful commit. It requires synchronous completion and an independent
// commit for this operation; surrounding operations may still fold.
// Backend-generated fields are excluded.
func (o WriteOperation) WithReturnedDocument() WriteOperation {
	o.returnDocument = true
	return o
}

func NewPut(address Address, document Document, mode WriteMode) (WriteOperation, error) {
	var operation WriteOperation
	if err := address.validate(); err != nil {
		return operation, err
	}
	if err := document.validate(); err != nil {
		return operation, err
	}
	if mode != WriteCreate && mode != WriteReplace && mode != WriteUpsert {
		return operation, errors.New("put operation has an invalid write mode")
	}
	operation = WriteOperation{
		address: address,
		action:  writeActionPut,
		put:     document,
		mode:    mode,
	}
	return operation, nil
}

func NewMerge(address Address, opts MergeOptions) (WriteOperation, error) {
	var operation WriteOperation
	if err := address.validate(); err != nil {
		return operation, err
	}
	if err := opts.Incoming.validate(); err != nil {
		return operation, err
	}
	if err := opts.Program.validate(); err != nil {
		return operation, err
	}
	operation = WriteOperation{
		address: address,
		action:  writeActionMerge,
		merge: mergeOperation{
			incoming: opts.Incoming,
			program:  opts.Program,
		},
	}
	return operation, nil
}

func (o WriteOperation) validate() error {
	if err := o.address.validate(); err != nil {
		return err
	}
	switch o.action {
	case writeActionPut:
		if err := o.put.validate(); err != nil {
			return err
		}
		if o.mode != WriteCreate && o.mode != WriteReplace && o.mode != WriteUpsert {
			return errors.New("put operation has an invalid write mode")
		}
	case writeActionMerge:
		if err := o.merge.incoming.validate(); err != nil {
			return err
		}
		if err := o.merge.program.validate(); err != nil {
			return err
		}
	default:
		return errors.New("write action is required")
	}
	return nil
}

type ReadResult struct {
	OperationIndex int
	Status         ReadStatus
	Document       Document
	Failure        *OperationError
}

type WriteResult struct {
	OperationIndex int
	Status         WriteStatus
	Failure        *OperationError
	Document       Document
}

type DeleteResult struct {
	OperationIndex int
	Status         DeleteStatus
	Failure        *OperationError
}

// OperationError reports a per-record failure without discarding successful
// results from the same batch.
type OperationError struct {
	OperationIndex int
	Code           FailureCode
	Message        string
	Retryable      bool
}

func (e *OperationError) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("sink operation %d failed with %s: %s", e.OperationIndex, e.Code, e.Message)
}

func (r ReadResult) Err() error {
	if r.Failure == nil {
		return nil
	}
	return r.Failure
}

func (r WriteResult) Err() error {
	if r.Failure == nil {
		return nil
	}
	return r.Failure
}

func (r DeleteResult) Err() error {
	if r.Failure == nil {
		return nil
	}
	return r.Failure
}

// BatchError collects per-operation failures while preserving errors.Is and
// errors.As traversal through every failure.
type BatchError struct {
	Failures []*OperationError
}

func (e *BatchError) Error() string {
	if e == nil || len(e.Failures) == 0 {
		return ""
	}
	return fmt.Sprintf("sink batch contains %d failed operations", len(e.Failures))
}

func (e *BatchError) Unwrap() []error {
	if e == nil {
		return nil
	}
	errorsList := make([]error, 0, len(e.Failures))
	for _, failure := range e.Failures {
		errorsList = append(errorsList, failure)
	}
	return errorsList
}

func ReadResultsError(results []ReadResult) error {
	failures := make([]*OperationError, 0)
	for _, result := range results {
		if result.Failure != nil {
			failures = append(failures, result.Failure)
		}
	}
	return newBatchError(failures)
}

func WriteResultsError(results []WriteResult) error {
	failures := make([]*OperationError, 0)
	for _, result := range results {
		if result.Failure != nil {
			failures = append(failures, result.Failure)
		}
	}
	return newBatchError(failures)
}

func DeleteResultsError(results []DeleteResult) error {
	failures := make([]*OperationError, 0)
	for _, result := range results {
		if result.Failure != nil {
			failures = append(failures, result.Failure)
		}
	}
	return newBatchError(failures)
}

func newBatchError(failures []*OperationError) error {
	if len(failures) == 0 {
		return nil
	}
	batchError := &BatchError{Failures: failures}
	return batchError
}

// ProtocolError means the server returned a structurally invalid response.
type ProtocolError struct {
	Method  string
	Message string
}

func (e *ProtocolError) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("sink %s response is invalid: %s", e.Method, e.Message)
}
