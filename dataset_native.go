package sink

import (
	"context"
	"errors"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// NewBSONCommand encodes a collection command for this Dataset. Arguments
// contain only the remaining command fields, e.g. filter, pipeline or indexes.
// The Store binds the empty collection placeholder using the resource URI.
// Ordered arguments and native BSON types are retained.
func (d *Dataset) NewBSONCommand(operation string, arguments bson.D) (Command, error) {
	var empty Command
	if err := d.validate("BSON command"); err != nil {
		return empty, err
	}
	if strings.TrimSpace(operation) == "" {
		return empty, errors.New("dataset BSON command requires an operation")
	}
	command := bson.D{{Key: operation, Value: ""}}
	command = append(command, arguments...)
	return NewBSONCommand(d.uri, command)
}

// Execute binds a native command to this Dataset's resource URI. The Store
// interprets the resource path and validates the command against that resource.
func (d *Dataset) Execute(ctx context.Context, req ExecuteRequest) (ExecuteResponse, error) {
	command, err := d.bindNativeCommand(req.Command)
	if err != nil {
		var empty ExecuteResponse
		return empty, err
	}
	req.Command = command
	return d.client.Execute(ctx, req)
}

// Query fetches a page from this Dataset. An empty Command selects all records.
// The Store selects its default query. Query controls and native payload
// semantics are the same as Client.Query.
func (d *Dataset) Query(ctx context.Context, req QueryRequest, callbacks ...DocumentCallback) (QueryResponse, error) {
	command, err := d.bindNativeCommand(req.Command)
	if err != nil {
		var empty QueryResponse
		return empty, err
	}
	req.Command = command
	return d.client.Query(ctx, req, callbacks...)
}

// Count counts matches in this Dataset. An empty Command counts all records;
// ordinary unfiltered MongoDB counts automatically use collection metadata.
func (d *Dataset) Count(ctx context.Context, req CountRequest) (CountResponse, error) {
	command, err := d.bindNativeCommand(req.Command)
	if err != nil {
		var empty CountResponse
		return empty, err
	}
	req.Command = command
	return d.client.Count(ctx, req)
}

// Scan returns one live page scoped to this Dataset. Reuse the request with
// NextCursor to continue. JSON commands must provide a unique stable sort.
func (d *Dataset) Scan(ctx context.Context, req ScanRequest, callbacks ...DocumentCallback) (ScanResponse, error) {
	command, err := d.bindNativeCommand(req.Command)
	if err != nil {
		var empty ScanResponse
		return empty, err
	}
	req.Command = command
	return d.client.Scan(ctx, req, callbacks...)
}

func (d *Dataset) bindNativeCommand(command Command) (Command, error) {
	var empty Command
	if err := d.validate("native command"); err != nil {
		return empty, err
	}
	if command.URI != "" && command.URI != d.uri {
		return empty, errors.New("native command URI differs from Dataset resource")
	}
	command.URI = d.uri
	if command.ContentType == "" && len(command.Payload) > 0 {
		switch d.encoding {
		case DocumentEncodingBSON:
			command.ContentType = "application/bson"
		case DocumentEncodingJSON:
			command.ContentType = "application/json"
		}
	}
	return command, nil
}
