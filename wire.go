package sink

import (
	"bytes"
	"fmt"

	sinkv1 "github.com/liran/sink-go/api/sink/v1"
)

func (a Address) toProto() *sinkv1.RecordAddress {
	address := &sinkv1.RecordAddress{Uri: a.URI()}
	return address
}

func (d Document) toProto() *sinkv1.Document {
	document := &sinkv1.Document{
		Encoding: d.encoding,
		Payload:  bytes.Clone(d.payload),
	}
	return document
}

func documentFromProto(document *sinkv1.Document) (Document, error) {
	var empty Document
	if document == nil {
		return empty, fmt.Errorf("document is missing")
	}
	return NewRawDocument(document.GetEncoding(), document.GetPayload())
}

func (o WriteOperation) toProto() *sinkv1.WriteOperation {
	operation := &sinkv1.WriteOperation{Address: o.address.toProto(), ReturnDocument: o.returnDocument}
	switch o.action {
	case writeActionPut:
		put := &sinkv1.PutOperation{
			Document: o.put.toProto(),
			Mode:     o.mode,
		}
		action := &sinkv1.WriteOperation_Put{Put: put}
		operation.Action = action
	case writeActionMerge:
		program := &sinkv1.LuaProgram{
			Sha256: o.merge.program.SHA256(),
		}
		merge := &sinkv1.MergeOperation{
			IncomingDocument: o.merge.incoming.toProto(),
			LuaProgram:       program,
		}

		action := &sinkv1.WriteOperation_Merge{Merge: merge}
		operation.Action = action
	}
	return operation
}
