package sink

import (
	"fmt"

	sinkv1 "github.com/batchstream/sink-go/api/sink/v1"
)

func decodeReadResult(protoResult *sinkv1.ReadResult, index int) (ReadResult, error) {
	var empty ReadResult
	if protoResult == nil {
		return empty, protocolError("Read", "result is empty")
	}
	result := ReadResult{OperationIndex: index, Status: protoResult.GetStatus()}
	switch result.Status {
	case ReadFound:
		document, documentErr := documentFromProto(protoResult.GetDocument())
		if documentErr != nil {
			return empty, protocolError("Read", fmt.Sprintf("result %d: %v", index, documentErr))
		}
		result.Document = document
	case ReadNotFound:
	case ReadFailed:
		failure, failureErr := operationFailure(index, protoResult.GetFailure())
		if failureErr != nil {
			return empty, protocolError("Read", failureErr.Error())
		}
		result.Failure = failure
	default:
		message := fmt.Sprintf("result %d has unsupported status %s", index, result.Status)
		return empty, protocolError("Read", message)
	}
	return result, nil
}

func decodeWriteResult(protoResult *sinkv1.WriteResult, index int) (WriteResult, error) {
	var empty WriteResult
	if protoResult == nil {
		return empty, protocolError("Write", "result is empty")
	}
	result := WriteResult{
		OperationIndex: index,
		Status:         protoResult.GetStatus(),
	}
	if protoResult.GetDocument() != nil {
		if result.Status != WriteApplied {
			return empty, protocolError("Write", "uncommitted result contains a document")
		}
		document, err := documentFromProto(protoResult.GetDocument())
		if err != nil {
			return empty, protocolError("Write", err.Error())
		}
		result.Document = document
	}
	switch result.Status {
	case WriteAccepted, WriteApplied:
	case WritePreconditionFailed, WriteFailed:
		failure, failureErr := operationFailure(index, protoResult.GetFailure())
		if failureErr != nil {
			return empty, protocolError("Write", failureErr.Error())
		}
		result.Failure = failure
	default:
		message := fmt.Sprintf("result %d has unsupported status %s", index, result.Status)
		return empty, protocolError("Write", message)
	}
	return result, nil
}

func decodeDeleteResponse(response *sinkv1.DeleteResponse, count int) ([]DeleteResult, error) {
	if response == nil {
		return nil, protocolError("Delete", "response is empty")
	}
	if len(response.GetResults()) != count {
		message := fmt.Sprintf("returned %d results for %d operations", len(response.GetResults()), count)
		return nil, protocolError("Delete", message)
	}
	results := make([]DeleteResult, count)
	seen := make([]bool, count)
	for _, protoResult := range response.GetResults() {
		if protoResult == nil {
			return nil, protocolError("Delete", "result is empty")
		}
		index, err := validateResultIndex("Delete", protoResult.GetOperationIndex(), seen)
		if err != nil {
			return nil, err
		}
		result := DeleteResult{OperationIndex: index, Status: protoResult.GetStatus()}
		switch result.Status {
		case DeleteAccepted, DeleteApplied:
		case DeleteFailed:
			failure, failureErr := operationFailure(index, protoResult.GetFailure())
			if failureErr != nil {
				return nil, protocolError("Delete", failureErr.Error())
			}
			result.Failure = failure
		default:
			message := fmt.Sprintf("result %d has unsupported status %s", index, result.Status)
			return nil, protocolError("Delete", message)
		}
		results[index] = result
	}
	return results, nil
}

func validateResultIndex(method string, rawIndex uint32, seen []bool) (int, error) {
	if uint64(rawIndex) >= uint64(len(seen)) {
		message := fmt.Sprintf("result operation index %d is out of range", rawIndex)
		return 0, protocolError(method, message)
	}
	index := int(rawIndex)
	if seen[index] {
		message := fmt.Sprintf("result operation index %d is duplicated", index)
		return 0, protocolError(method, message)
	}
	seen[index] = true
	return index, nil
}

func operationFailure(index int, failure *sinkv1.Failure) (*OperationError, error) {
	if failure == nil {
		return nil, fmt.Errorf("result %d is missing failure details", index)
	}
	if failure.GetCode() == sinkv1.FailureCode_FAILURE_CODE_UNSPECIFIED {
		return nil, fmt.Errorf("result %d has an unspecified failure code", index)
	}
	if failure.GetMessage() == "" {
		return nil, fmt.Errorf("result %d has an empty failure message", index)
	}
	operationError := &OperationError{
		OperationIndex: index,
		Code:           failure.GetCode(),
		Message:        failure.GetMessage(),
		Retryable:      failure.GetRetryable(),
	}
	return operationError, nil
}

func protocolError(method string, message string) *ProtocolError {
	err := &ProtocolError{Method: method, Message: message}
	return err
}
