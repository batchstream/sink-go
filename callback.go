package sink

import "errors"

// ReadCallback consumes a final read result before the next receive. Returning
// an error cancels the RPC. The SDK retains no result documents when a callback
// is supplied. Already delivered results remain valid if the stream fails.
type ReadCallback func(ReadResult) error

// WriteCallback consumes a final write result without collecting documents.
// Returning an error cancels the RPC. Unacknowledged writes have an unknown
// outcome and are never replayed by the SDK.
type WriteCallback func(WriteResult) error

// DocumentCallback consumes a Query or Scan document in query order.
// Returning an error cancels the RPC. The SDK does not collect these documents.
type DocumentCallback func(Document) error

func oneCallback[T any](callbacks []T) (T, error) {
	var empty T
	if len(callbacks) > 1 {
		return empty, errors.New("at most one result callback is allowed")
	}
	if len(callbacks) == 0 {
		return empty, nil
	}
	return callbacks[0], nil
}
