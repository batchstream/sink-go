package sink

// Request helpers return an updated value for chaining. They do not modify the
// original request or copy document payloads. Validation happens when calling
// Client or Dataset, just as it does for requests built with struct literals.

const defaultPageSize = 100

// NewReadRequest selects records to collect in request order by default.
func NewReadRequest(addresses ...Address) ReadRequest {
	request := ReadRequest{Addresses: addresses}
	return request
}

// WithOnResult consumes results without collecting them. Nil restores collection.
func (r ReadRequest) WithOnResult(callback ReadCallback) ReadRequest {
	r.OnResult = callback
	return r
}

// NewWriteRequest submits operations with APPLIED completion by default.
func NewWriteRequest(operations ...WriteOperation) WriteRequest {
	request := WriteRequest{CompletionMode: CompletionWaitUntilApplied, Operations: operations}
	return request
}

// WithCompletionMode selects when writes are acknowledged. Zero means APPLIED.
func (r WriteRequest) WithCompletionMode(mode CompletionMode) WriteRequest {
	r.CompletionMode = mode
	return r
}

// WithOnResult consumes acknowledgements without collecting them. Nil restores collection.
func (r WriteRequest) WithOnResult(callback WriteCallback) WriteRequest {
	r.OnResult = callback
	return r
}

// NewDeleteRequest deletes records with APPLIED completion by default.
func NewDeleteRequest(addresses ...Address) DeleteRequest {
	request := DeleteRequest{CompletionMode: CompletionWaitUntilApplied, Addresses: addresses}
	return request
}

// WithCompletionMode selects when deletes are acknowledged. Zero means APPLIED.
func (r DeleteRequest) WithCompletionMode(mode CompletionMode) DeleteRequest {
	r.CompletionMode = mode
	return r
}

// NewDatasetReadRequest selects keys to collect in request order by default.
func NewDatasetReadRequest(keys ...Key) DatasetReadRequest {
	request := DatasetReadRequest{Keys: keys}
	return request
}

// WithOnResult consumes results without collecting them. Nil restores collection.
func (r DatasetReadRequest) WithOnResult(callback ReadCallback) DatasetReadRequest {
	r.OnResult = callback
	return r
}

// NewDatasetWriteRequest supplies records with APPLIED completion by default.
func NewDatasetWriteRequest(records ...Record) DatasetWriteRequest {
	request := DatasetWriteRequest{CompletionMode: CompletionWaitUntilApplied, Records: records}
	return request
}

// WithCompletionMode selects when mutations are acknowledged. Zero means APPLIED.
func (r DatasetWriteRequest) WithCompletionMode(mode CompletionMode) DatasetWriteRequest {
	r.CompletionMode = mode
	return r
}

// WithOnResult consumes acknowledgements without collecting them. Nil restores collection.
func (r DatasetWriteRequest) WithOnResult(callback WriteCallback) DatasetWriteRequest {
	r.OnResult = callback
	return r
}

// NewQueryRequest selects page 1 with 100 documents and collection enabled.
// Dataset supplies the resource; Client callers must supply a Command.
func NewQueryRequest() QueryRequest {
	request := QueryRequest{Page: 1, PageSize: defaultPageSize}
	return request
}

// WithCommand supplies the native query and resource.
func (r QueryRequest) WithCommand(command Command) QueryRequest {
	r.Command = command
	return r
}

// WithPage selects a one-based page. Zero means page 1.
func (r QueryRequest) WithPage(page int) QueryRequest {
	r.Page = page
	return r
}

// WithPageSize selects the page size. Zero means 100; the maximum is 1000.
func (r QueryRequest) WithPageSize(size int) QueryRequest {
	r.PageSize = size
	return r
}

// WithSort replaces the native sort with the supplied ordered fields.
// No fields preserves the native sort.
func (r QueryRequest) WithSort(fields ...SortField) QueryRequest {
	r.Sort = fields
	return r
}

// WithProjection selects returned fields. Nil preserves native projection.
func (r QueryRequest) WithProjection(projection *Projection) QueryRequest {
	r.Projection = projection
	return r
}

// WithOnDocument consumes documents without collecting them. Nil restores collection.
// Page metadata is still returned after successful completion.
func (r QueryRequest) WithOnDocument(callback DocumentCallback) QueryRequest {
	r.OnDocument = callback
	return r
}

// NewScanRequest starts a scan with 100 documents per page and collection enabled.
// Dataset supplies the resource; Client callers must supply a Command.
func NewScanRequest() ScanRequest {
	request := ScanRequest{BatchSize: defaultPageSize}
	return request
}

// WithCommand supplies the native query and resource.
func (r ScanRequest) WithCommand(command Command) ScanRequest {
	r.Command = command
	return r
}

// WithBatchSize selects the page size. Zero means 100; the maximum is 1000.
func (r ScanRequest) WithBatchSize(size int) ScanRequest {
	r.BatchSize = size
	return r
}

// WithCursor resumes from a committed checkpoint. Empty starts a new scan.
func (r ScanRequest) WithCursor(cursor []byte) ScanRequest {
	r.Cursor = cursor
	return r
}

// WithProjection selects returned fields. Nil preserves native projection.
func (r ScanRequest) WithProjection(projection *Projection) ScanRequest {
	r.Projection = projection
	return r
}

// WithOnDocument consumes documents without collecting them. Nil restores collection.
// The next cursor is still returned after successful completion.
func (r ScanRequest) WithOnDocument(callback DocumentCallback) ScanRequest {
	r.OnDocument = callback
	return r
}

func defaultCompletionMode(mode CompletionMode) CompletionMode {
	if mode == 0 {
		return CompletionWaitUntilApplied
	}
	return mode
}
