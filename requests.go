package sink

// ReadRequest selects records and how their results are consumed.
type ReadRequest struct {
	Addresses []Address
	// OnResult consumes each final result without collecting a result slice.
	// Nil collects results in request order. Returning an error cancels the stream.
	OnResult ReadCallback
}

// WriteRequest submits mixed put and merge operations with one completion mode.
type WriteRequest struct {
	// CompletionMode defaults to CompletionWaitUntilApplied when zero.
	CompletionMode CompletionMode
	Operations     []WriteOperation
	// OnResult consumes each acknowledgement without collecting a result slice.
	// Nil collects results in request order. Returning an error cancels the stream;
	// unacknowledged operations may have completed and are never replayed.
	OnResult WriteCallback
}

// DeleteRequest permanently deletes records with one completion mode.
type DeleteRequest struct {
	// CompletionMode defaults to CompletionWaitUntilApplied when zero.
	CompletionMode CompletionMode
	Addresses      []Address
}

// DatasetReadRequest selects keys within the Dataset's bound resource.
type DatasetReadRequest struct {
	Keys []Key
	// OnResult has the same delivery and collection semantics as ReadRequest.OnResult.
	OnResult ReadCallback
}

// DatasetWriteRequest supplies records for Create, Replace, Upsert or Merge.
// Routing, encoding and merge programs come from the Dataset.
type DatasetWriteRequest struct {
	// CompletionMode defaults to CompletionWaitUntilApplied when zero.
	CompletionMode CompletionMode
	Records        []Record
	// OnResult has the same delivery and collection semantics as WriteRequest.OnResult.
	OnResult WriteCallback
}
