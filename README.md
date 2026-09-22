# sink-go

`sink-go` is the typed, concurrency-safe Go client for the
[`batchstream/sink`](https://github.com/batchstream/sink) gRPC service. It covers the full
batch API: reads, puts, self-contained Lua merges, and hard deletes with synchronous
or durable asynchronous completion.
Native `Execute` commands and resumable `Scan` pages use the
same Sink connection, while returned writes support atomic read-modify-write results.

## Install

```shell
go get github.com/batchstream/sink-go
```

The client requires Go 1.27 or newer.

## Record addresses

Record addresses are canonical `sink://<store>/<store-defined-path>` URIs.
`NewAddress` parses a complete URI; `NewRecordAddress` appends a typed key to a
resource URI. `DatasetOptions.URI` is the resource URI without its final key.

```go
address, err := sink.NewAddress("sink://primary/catalog/products/s:product-42")
address, err = sink.NewRecordAddress("sink://search-main/products", sink.StringKey("product/42"))
```

MongoDB paths are `database/collection/typed-key`; search paths are
`index/typed-key`. The Gateway treats the path as opaque and uses the full
canonical URI for Engine affinity. All Gateways with the same Engine membership
select the same Engine for a record. Membership changes may temporarily split
traffic. No exclusive ownership or cross-replica ordering is promised.

The shared `github.com/batchstream/sink-go/uri` package builds, parses and validates
URIs. String keys use `s:`, int64 keys `i:`, bytes keys `b:` plus unpadded base64url,
and opaque keys `o:<base64url type>:<base64url data>`. The builder escapes each
path segment; encoded slashes are part of a segment. Alternate URI spellings,
empty/dot segments, invalid UTF-8 and noncanonical typed keys are rejected.
Store names are lowercase ASCII letters/digits with `.`, `_` and `-` after the
first character. The entire URI is at most 16 KiB.

Use this SDK with a Sink build that implements the same protocol.
Native `Command` fields remain backend-specific and do not use record affinity.

## Quick start

`Dial` uses TLS 1.2 or newer by default. The explicit insecure credentials in
this example are appropriate only for a trusted local endpoint.

```go
package main

import (
	"context"
	"log"
	"time"

	sink "github.com/batchstream/sink-go"
	"google.golang.org/grpc/credentials/insecure"
)

type Product struct {
	UID       string    `json:"uid" bson:"_id"`
	Name      string    `json:"name" bson:"name"`
	Stock     int       `json:"stock" bson:"stock"`
	UpdatedAt time.Time `json:"updated_at" bson:"updated_at"`
}

func main() {
	dialOptions := sink.DialOptions{
		TransportCredentials: insecure.NewCredentials(),
	}
	client, err := sink.Dial("127.0.0.1:8080", dialOptions)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	datasetOptions := sink.DatasetOptions{
		URI:       "sink://primary/catalog/products",
		Encoding:  sink.DocumentEncodingBSON,
	}
	products, err := sink.NewDataset(client, datasetOptions)
	if err != nil {
		log.Fatal(err)
	}
	value := Product{
		UID:       "product-42",
		Name:      "keyboard",
		Stock:     12,
		UpdatedAt: time.Now().UTC(),
	}
	record := sink.Record{
		Key:   sink.StringKey(value.UID),
		Value: value,
	}
	writeRequest := sink.NewDatasetWriteRequest(record).
		WithCompletionMode(sink.CompletionWaitUntilVisible)
	_, err = products.Upsert(context.Background(), writeRequest)
	if err != nil {
		log.Fatal(err)
	}
	readRequest := sink.NewDatasetReadRequest(sink.StringKey(value.UID))
	readResults, err := products.Read(context.Background(), readRequest)
	if err != nil {
		log.Fatal(err)
	}
	if readResults[0].Status == sink.ReadFound {
		var stored Product
		if err := readResults[0].Document.Decode(&stored); err != nil {
			log.Fatal(err)
		}
	}
}
```

`Dataset` binds a resource URI, document encoding, and optional merge program.
`Read` accepts one or many keys without reconstructing addresses. Mutations
wait until applied by default; `WithCompletionMode` changes durability or
visibility for an individual call. Pass records to `NewDatasetWriteRequest` or
set `DatasetWriteRequest.Records` directly. The client validates and encodes the
complete collection before sending it, automatically splits large collections
by `ClientOptions.MaxOperations`, and preserves global operation indexes:

```go
records := []sink.Record{
	{Key: sink.StringKey("product-42"), Value: firstProduct},
	{Key: sink.StringKey("product-43"), Value: secondProduct},
}
request := sink.NewDatasetWriteRequest(records...).
	WithCompletionMode(sink.CompletionReturnAfterAccepted)
results, err := products.Upsert(context.Background(), request)
```

Configure `DocumentEncodingBSON` for MongoDB; it applies `bson` tags and keeps
native values such as BSON datetimes. Configure `DocumentEncodingJSON` for
Elasticsearch and OpenSearch; it applies `json` tags and produces ordinary JSON
without Extended JSON `$date` wrappers. Sink rejects an encoding that does not
match the selected backend. The low-level API also requires callers to create an
explicitly encoded `Document` before `NewPut` or `NewMerge`.

Reads return a `Document` with `Encoding`, immutable `Payload`, and `Decode`
methods. `Decode` selects the matching JSON or BSON decoder automatically.

A merge rule is ordinary application source code. Construct its immutable
`LuaProgram` once and bind it to the `Dataset`; every `Merge` call reuses that
program while the client includes its SHA-256 digest automatically:

```go
source := []byte(`
return function(current, incoming)
    current = current or json.object()
    current.stock = incoming.stock
    current.updated_at = sink.v1.time.now()
    return current
end`)
program, err := sink.NewLuaProgram(source)
if err != nil {
	log.Fatal(err)
}
datasetOptions := sink.DatasetOptions{
	URI:          "sink://search-main/products",
	Encoding:     sink.DocumentEncodingJSON,
	MergeProgram: &program,
}
products, err := sink.NewDataset(client, datasetOptions)
if err != nil {
	log.Fatal(err)
}
record := sink.Record{
	Key:   sink.StringKey("product-42"),
	Value: incomingProduct,
}
request := sink.NewDatasetWriteRequest(record).
	WithCompletionMode(sink.CompletionWaitUntilVisible)
results, err := products.Merge(context.Background(), request)
if err != nil {
	log.Fatal(err)
}
```

The stored current document and incoming records must use the Dataset's
encoding, and the merge result preserves it. A Dataset without `MergeProgram`
rejects `Merge` before sending an RPC. When no stored document exists, the Lua
function receives `nil` as `current` and its returned object is created.

The merge function receives only `current` and `incoming`. Sink provides
versioned `sink.v1` array, object, and retry-stable time helpers. See the
[Lua merge developer guide](https://github.com/batchstream/sink/blob/main/docs/lua-merge-guide.md)
for the complete function reference and reliability rules.

## Request helpers and defaults

Helpers accept individual records/keys/addresses, or an existing slice with
`...`. Chain `With...` methods to override fields; each method returns an updated
request value without modifying the original or copying document payloads.
Direct struct literals remain supported and receive the same defaults at call time.

```go
request := sink.NewDatasetWriteRequest(records...) // APPLIED; collect results.
results, err := products.Upsert(ctx, request)

streamRequest := request.WithOnResult(processWrite)
results, err = products.Upsert(ctx, streamRequest) // results == nil

readRequest := sink.NewDatasetReadRequest(keys...).WithOnResult(processRead)
readResults, err := products.Read(ctx, readRequest) // readResults == nil

queryRequest := sink.NewQueryRequest().WithPage(2).WithPageSize(50)
page, err := products.Query(ctx, queryRequest)

scanRequest := sink.NewScanRequest().WithBatchSize(50).WithOnDocument(processDocument)
scanPage, err := products.Scan(ctx, scanRequest)
if err != nil {
    return err
}
scanRequest = scanRequest.WithCursor(scanPage.NextCursor)
```

| Request field | Default when unset |
| --- | --- |
| Write/Delete/DatasetWrite `CompletionMode` | `CompletionWaitUntilApplied` |
| Query `Page` / `PageSize` | `1` / `100` |
| Scan `BatchSize` / `Cursor` | `100` / start a new scan |
| `OnResult` / `OnDocument` | Collect and return results |
| Query `Sort` / Query and Scan `Projection` | Preserve native settings |

Client record methods have corresponding `NewReadRequest`, `NewWriteRequest`
and `NewDeleteRequest` helpers. Native Client queries and scans use
`WithCommand(command)`; Dataset supplies its bound resource when Command is empty.
`WithSort` keeps the given field order, and `WithProjection` accepts the existing
Projection type. Nil callbacks restore collection. Invalid nonzero completion
modes, negative pagination values and oversized pages still fail validation.

## Streaming callbacks

Read, Write, Query and Scan use server-streaming RPCs. Each method accepts
`ctx` and a typed request. Record requests use `Addresses`, `Operations`, `Keys`
or `Records` slices; completion mode and callbacks belong to the same request.
Future options can be added as fields without changing method signatures.

`OnResult` handles Read/Write results and `OnDocument` handles Query/Scan documents.
A nil callback collects and returns results. A non-nil callback processes each
item serially without collecting it: Read/Write return a nil result slice;
Query/Scan return nil `Documents` while retaining page metadata on success.

```go
readRequest := sink.ReadRequest{Addresses: addresses}
results, err := client.Read(ctx, readRequest) // Collected in request order.

readRequest.OnResult = func(result sink.ReadResult) error {
    // Returning an error cancels the stream.
    return result.Err()
}
results, err = client.Read(ctx, readRequest) // results == nil

scanRequest := sink.ScanRequest{
    Command: command,
    OnDocument: func(document sink.Document) error {
        return process(document)
    },
}
page, err := client.Scan(ctx, scanRequest)
// page.Documents == nil; NextCursor is available only on success.
```

Dataset Read uses `DatasetReadRequest`; Create/Replace/Upsert/Merge share
`DatasetWriteRequest`. Both expose `OnResult`. Query and Scan share their request
types with Client and expose `OnDocument`. Different record results can arrive
out of order; `OperationIndex` always refers to the original input slice.
No callback documents are retained by the SDK. Dataset methods retain only
failure metadata for `BatchError`.

Callbacks can observe partial results before a stream fails. Read retries never
redeliver completed results, and callback errors are never retried. Writes are
never automatically replayed; undelivered outcomes are unknown. Query/Scan
metadata is published only after successful EOF. If a page fails after processing
some documents, keep the previous cursor and use idempotent processing on resume.
The default collector returns received documents/results along with any error.

## API model

- `Dataset` is the primary record API. It binds routing, encoding, and one
  optional Lua program. `Read` accepts one or many keys; `Create`, `Replace`,
  `Upsert`, and `Merge` accept one or many `Record` values with APPLIED completion
  by default and an optional per-call override. Every method splits large collections
  automatically and collects per-record results unless a callback is supplied.
- `Read(ctx, ReadRequest)` preserves request order and reports found,
  not-found, or failed results independently.
- `Write(ctx, WriteRequest)` supports mixed put and merge
  batches. Use `NewPut` and `NewMerge` to construct validated operations.
- `Delete(ctx, DeleteRequest)` performs hard deletes; deleting
  an absent record is successful.
- `Execute(ctx, request)` returns native BSON or HTTP payloads, status and headers
  for native queries, writes, and administration; MongoDB cursor/session commands
  are rejected.
- `Scan(ctx, request)` returns one live page of MongoDB documents or complete
  search hits, with an opaque `NextCursor` for resuming on any server.
- String, int64, byte, and opaque keys are supported.
- `CheckHealth` uses the standard gRPC health service.
- `Raw` exposes the generated `api/sink/v1` client for advanced use.

Use `CompletionWaitUntilApplied` for storage acknowledgement,
`CompletionWaitUntilVisible` when a following search read must observe the
mutation, and `CompletionReturnAfterAccepted` for durable asynchronous queueing.
For OpenSearch and Elasticsearch, visible completion maps to
`refresh=wait_for`; MongoDB is immediately visible after an acknowledged write.

Lua source travels with the merge intent in both synchronous and asynchronous
mode. Sink caches compilation by digest while using a fresh VM per execution,
so rules follow application releases without server-side profile management.
The client declares identical source only once per `Write` batch; Sink embeds
the full program into each asynchronous Kafka mutation so it remains replayable
without server-side rule state.

Each result includes its operation index. The client validates result counts,
indexes, statuses, documents, and failure details before returning a response.
Per-record failures are represented by `OperationError`, so one bad record does
not hide successful records from the same batch. Dataset methods return all
available results and automatically expose operation failures as an
`errors.As`-compatible `BatchError`. If a later split batch also has a transport
failure, the returned error preserves both the earlier operation failures and
the transport error. Low-level results expose `Err()`, and `ReadResultsError`,
`WriteResultsError`, and `DeleteResultsError` collect failures explicitly.

## Native commands, queries, counts, and scans

Construct ordered MongoDB commands without opening a database connection. A
struct, `bson.D`, or `bson.Raw` preserves the command field order; unordered maps
are rejected by `NewBSONCommand`. All native RPCs use the same `Command` fields
(URI, method, path, query, headers, content type and payload). BSON values are retained through the RPC:

```go
commandValue := struct {
	Count string `bson:"count"`
}{Count: "products"}
command, err := sink.NewBSONCommand("sink://primary/catalog", commandValue)
if err != nil {
	return err
}
request := sink.ExecuteRequest{Command: command}
response, err := client.Execute(ctx, request)
if err != nil {
	var native *sink.NativeError
	if errors.As(err, &native) {
		// native.Response.Payload retains the database's complete BSON error.
	}
	return err
}
var count struct {
	N int64 `bson:"n"`
}
if err := response.Decode(&count); err != nil {
	return err
}
```

For search, put the resource in URI and the operation in Path: for example,
`URI: "sink://search-main/products"`, `Path: "/_search"`. Pass the HTTP method,
URL-encoded query parameters and original body separately. Sink uses its configured endpoint and credentials. For example:

```go
command := sink.Command{
	URI: "sink://search-main/products",
	ContentType: "application/json",
	Method: "POST",
	Path:   "/_search",
	Query:  "track_total_hits=true",
	Payload: []byte(`{"query":{"match":{"name":"keyboard"}},"size":20}`),
}
request := sink.ExecuteRequest{Command: command}
response, err := client.Execute(ctx, request)
// response.Payload, StatusCode, and Headers remain available on NativeError.
```

MongoDB Execute supports `insert`, `update`,
`delete`, and `findAndModify`, plus an explicit set of read/diagnostic and index
commands. Inserted/replacement documents, operator updates, and update pipelines
atomically receive fresh Sink revisions, so concurrent record Merges detect the
change and recompute. The server rejects metadata tampering, unsafe commands such
as `drop`/`renameCollection`, and unknown commands before execution; it does not
fall back to unrestricted passthrough. All writers to a collection must use the
same revision protocol and metadata field.

Search Execute validates supported routes before forwarding native payloads.
Document writes, `_bulk`, queries, mapping updates and routine refresh/flush
operations remain available. Index lifecycle operations, alias changes,
settings/templates, lifecycle policies and unknown administrative/plugin write
routes return `INVALID_ARGUMENT` before reaching the backend. GET/HEAD/OPTIONS
can still inspect native endpoints. See the server's
[native access contract](https://github.com/batchstream/sink/blob/main/docs/native-access.md#elasticsearch-and-opensearch-endpoints)
for supported routes. External administration and
existing lifecycle policies must be coordinated with record clients, which must
discard old revisions and snapshots after an index change.
Native mutations do not run Lua merges or
participate in record batching or asynchronous completion modes. Neither the
server nor this SDK automatically retries or deduplicates native mutations.

MongoDB cursor commands (`find`, `aggregate`, `listIndexes`, `listCollections`,
`getMore`, `killCursors`, `parallelCollectionScan`, and cursor-returning
`bulkWrite`) are rejected by Execute. Use Scan for resumable find pages and Query for
independent find/aggregate pages.
Client-managed sessions and transactions are also unsupported. Search scrolls
can be managed explicitly through Execute, with cleanup owned by the caller.

`ExecuteResponse.Decode` handles JSON and BSON; other content types can use
`Payload` directly. Nonempty request payloads require `ContentType`; set it
directly instead of in Headers. `_msearch` uses `application/x-ndjson` with the
final newline intact.
Database failures return both the response and `*NativeError`. Transport errors
return no response. HTTP 2xx can still contain partial search failures, so inspect
the raw response when using `_msearch` or other partial-result queries.

Fetch independent pages with explicit sorting and projection, and request the
count separately when needed. Reuse the same native query Command for both:

```go
command := sink.Command{
 URI: "sink://search-main/products",
 Method: "POST",
 Path: "/_search",
 ContentType: "application/json",
 Payload: []byte(`{"query":{"match":{"name":"keyboard"}}}`),
}
projection := &sink.Projection{Fields: []string{"name", "price", "uid"}}
request := sink.QueryRequest{
 Command: command,
 Page: 2,
 PageSize: 20,
 Sort: []sink.SortField{{Field: "price", Descending: true}, {Field: "uid"}},
 Projection: projection,
}
page, err := client.Query(ctx, request)
if err != nil {
 return err
}
// page.Documents contains complete native hits with projected _source fields.
// page.HasMore is determined by fetching one extra result, without counting.
countRequest := sink.CountRequest{Command: command}
count, err := client.Count(ctx, countRequest)
// count.Count is the total; count.Estimated identifies a metadata estimate.
```

Pages start at 1; zero defaults to page 1 and page size 100, with a maximum of
1000. Empty Sort preserves native ordering. Nil Projection preserves the native
projection; an explicit empty field list selects all fields. Set Exclude to
exclude listed fields. MongoDB uses native document paths and `_id` projection
rules; HTTP projection selects `_source` fields and retains hit metadata.

Query overrides find skip/limit or HTTP from/size. Explicit sort/projection replace
native settings; for aggregate, they apply after the supplied pipeline and before
pagination. Both Query and Count support find/read-only aggregate and HTTP _search.
Count ignores find/HTTP pagination; aggregate Count counts pipeline output. HTTP
Count counts matching documents before collapse. Use Execute for full native replies.

For MongoDB find predicates using `$where`, `$near` or `$nearSphere`, updated
servers count a native find cursor with a constant projection instead of
translating the filter into an aggregation. This retains native filter semantics
and returns `Estimated=false`, but transfers one small result per match and may
be slower. Timeouts and cursor failures return errors without a partial total.

No cursor is retained between Query calls. Stable sorting with a unique tie-breaker
is recommended; concurrent writes can shift pages and change a separately requested
count. Deep pages remain subject to backend offset costs and result-window limits,
including the extra result for HasMore. Use Scan for full traversal. MongoDB Count
automatically uses `EstimatedDocumentCount` for missing or empty find filters without options
requiring exact execution.
The response exposes `Count` and `Estimated`, indicating whether an estimate was
used. Filtered queries, aggregate pipelines and HTTP searches remain exact.
Incomplete counts and approximate HTTP search totals fail. Query and Count return gRPC failures rather than NativeError,
and the SDK retries neither operation.

`Dataset` also exposes `Execute`, `Query`, `Count` and `Scan` using the same request
and response types as Client. The SDK binds the complete resource URI without
interpreting its path. Each Store adapter resolves the resource and supplies
default queries. Empty Query/Count commands select all records in the Dataset.
MongoDB Scan also accepts an empty Command; search Scan needs a body with
an explicit stable sort. Conflicting resource URIs or BSON collection targets fail.
The Dataset wrappers retain native error, retry and cursor semantics.

```go
opts := sink.DatasetOptions{
 URI: "sink://primary/catalog/products",
 Encoding: sink.DocumentEncodingBSON,
}
products, err := sink.NewDataset(client, opts)
if err != nil {
 return err
}
// The helper inserts the operation; the Store binds its collection from the URI.
arguments := bson.D{{Key: "filter", Value: bson.D{{Key: "active", Value: true}}}}
command, err := products.NewBSONCommand("find", arguments)
if err != nil {
 return err
}
projection := &sink.Projection{Fields: []string{"name", "price"}}
query := sink.QueryRequest{
 Command: command, Page: 2, PageSize: 20,
 Sort: []sink.SortField{{Field: "price"}, {Field: "_id"}}, Projection: projection,
}
page, err := products.Query(ctx, query)

// No command needed for the whole table; MongoDB automatically uses metadata.
countRequest := sink.CountRequest{}
count, err := products.Count(ctx, countRequest)

scanRequest := sink.ScanRequest{Command: command, BatchSize: 100}
scanPage, err := products.Scan(ctx, scanRequest)
// After processing scanPage.Documents, save scanPage.NextCursor for the next call.

// Collection commands use the same helper and Execute wrapper.
index := bson.D{{Key: "name", Value: "price"}, {Key: "key", Value: bson.D{{Key: "price", Value: 1}}}}
indexArguments := bson.D{{Key: "indexes", Value: bson.A{index}}}
indexCommand, err := products.NewBSONCommand("createIndexes", indexArguments)
if err != nil {
 return err
}
executeRequest := sink.ExecuteRequest{Command: indexCommand}
response, err := products.Execute(ctx, executeRequest)
```

An existing full BSON command can also be passed; its first value must match the
URI collection or be an empty string placeholder; the MongoDB adapter validates
and binds it. The SDK forwards the payload unchanged. The Dataset BSON helper
accepts ordered `bson.D` arguments and encodes the command once. Native BSON types
and ordered fields are preserved. For search Stores, supply an index-relative Path
such as `/_search`, `/_mapping` or `/_doc/id`; empty Execute Path selects the index
itself for inspection with GET/HEAD. Explicit index creation/deletion is rejected
by Sink. Query/Count/Scan default to `POST /<index>/_search`. For nonempty
payloads, ContentType defaults to the Dataset document encoding; explicit
ContentType takes precedence, including NDJSON for bulk bodies. Use Client for
database, cluster and multi-index endpoints. Dataset binding is a convenience;
it does not restrict cross-collection operations inside native payloads.

Scan returns one page at a time and does not accumulate the complete result set:

```go
command := sink.Command{
 URI: "sink://search-main/products",
 ContentType: "application/json",
 Method: "POST",
 Path: "/_search",
 Payload: []byte(`{"query":{"match_all":{}},"sort":[{"uid.keyword":"asc"}]}`),
}
projection := &sink.Projection{Fields: []string{"name", "price"}}
request := sink.ScanRequest{Command: command, BatchSize: 100, Projection: projection}
for {
 page, err := client.Scan(ctx, request)
 if err != nil {
  return err // Retain the last successfully processed checkpoint for retry.
 }
 for _, document := range page.Documents {
  var hit struct {
   ID string `json:"_id"`
   Source json.RawMessage `json:"_source"`
  }
  if err := document.Decode(&hit); err != nil {
   return err
  }
  if err := process(ctx, hit.ID, hit.Source); err != nil {
   return err
  }
 }
 if len(page.NextCursor) == 0 {
  break // Persist task completion; an empty cursor starts a new scan.
 }
 request.Cursor = page.NextCursor // Persist only after processing the whole page.
}
```

`ScanRequest.Projection` uses the same `Fields` and `Exclude` controls as Query.
Nil preserves native projection; a non-nil empty projection selects all fields.
Projection is executed by the backend, reducing document transfer and payload
memory. Search paths are relative to `_source`, and hit metadata is preserved.

MongoDB Scan supports find queries in `_id` ascending order by default, or an
explicit `_id` descending sort, with simple collation. Projections can exclude
`_id`; Sink still uses the original ID in the opaque cursor. Other sorts,
aggregates, skip/limit and list commands are not supported by Scan. Query retains
its independent find/aggregate pagination behavior.

Search Scan requires an explicit stable, globally unique sort tuple using
ordinary fields with doc_values. The example assumes `uid.keyword` is a unique
immutable keyword field. `_id`, `_doc`, `_shard_doc`, `_score`, null sort values,
scripted sorts and URL sort are unsupported. Every document is a complete JSON
hit, including its sort values. Scan uses search_after without scroll or PIT.

Resend the same Command and Projection, including headers and native payload
bytes, with `Cursor` set to the previous `NextCursor`. Batch size may change. Changing
Projection during continuation returns `INVALID_ARGUMENT`. Cursors are
opaque continuation markers limited to 64 KiB and bound to the query; they are
not credentials. They do not expire and survive Sink server restarts. There is
no keep-alive, explicit close operation or database cursor retained between
requests. Each request still has the server's ordinary deadline and admission
limits; task deadlines and checkpoint retention belong to the application.

Only an empty NextCursor marks the end, even if a byte-limited page is short.
Scan reads live data: inserts before the checkpoint may be missed, later inserts
may appear, and updates/deletes can change results. Retries from the same cursor
can observe newer data. Execute never retries automatically. Scan retries only
temporary admission rejections explicitly marked by Sink, using the identical
command and cursor. Unmarked `ResourceExhausted`,
backend errors, transport failures and invalid pages are not retried. A failed
Scan can return partial documents but no next cursor; reuse the last saved
cursor and process idempotently.
Persist task completion separately so a completed task does not restart from an
empty cursor. A dataset recreation or remapping requires an explicit new scan.

## Return the result of a write

Set `Record.ReturnDocument` for Dataset `Create`, `Replace`, `Upsert`, or `Merge`,
or use `operation.WithReturnedDocument()` with the low-level Write API:

```go
record := sink.Record{
	Key:            sink.StringKey("daily-quota"),
	Value:          increment,
	ReturnDocument: true,
}
request := sink.NewDatasetWriteRequest(record) // Defaults to APPLIED.
results, err := quotas.Merge(ctx, request)
if err != nil {
	return err
}
var quota Quota
if err := results[0].Document.Decode(&quota); err != nil {
	return err
}
```

Bind an increment Lua program to `quotas`. Each successful result contains that
operation's logical document. Sink takes it from the successful
commit candidate without a later Read. Each operation requesting a returned
document commits independently; other operations in the same-address chain
may still fold into one commit. Failed operations have no
document. Backend-generated fields and ingest transformations are excluded;
Read is available for a later stored observation. Returning documents requires
synchronous completion and is rejected with `CompletionReturnAfterAccepted`.
A timeout can still leave a mutation's outcome unknown; this is not exactly-once
increment delivery or a multi-document transaction.

See the server's [native access contract](https://github.com/batchstream/sink/blob/main/docs/native-access.md)
for cursor restrictions, native write semantics, raw metadata, byte limits, and
scan deadlines.

## Reliability behavior

`Dial` defaults to `round_robin` across the addresses returned by the resolver.
In Kubernetes, use a headless Service selecting only Sink Gateway pods and a
target such as `dns:///sink-headless.sink.svc.cluster.local:8080`. A normal
ClusterIP resolves to one virtual address and does not expose individual
replicas for per-RPC balancing. Resolver service configuration or explicit
`DialOptions.GRPCOptions` can override the default policy. For DNS targets,
`DialOptions.DNSRefreshInterval` controls the delay after a successful address
update before the next lookup while the channel is active. Zero defaults to
30 seconds, positive durations override it, and negative durations are rejected
by `Dial`. For example:

```go
dialOptions := sink.DialOptions{
	DNSRefreshInterval: 5 * time.Second,
}
client, err := sink.Dial("dns:///sink.example.com:8080", dialOptions)
```

Intervals below 30 seconds take effect without changing gRPC's process-global
resolution settings. Each scheduled refresh starts a new DNS resolver while
retaining the gRPC channel and unchanged backend connections. Slow or failed
lookups retain gRPC's DNS timeout and retry behavior; the next scheduled refresh
starts after an accepted update. Lookup delays and DNS-server caches can delay
discovery, so the configured interval is not an availability guarantee.
Literal IP targets perform no DNS queries and have no refresh timer. Explicit
resolvers supplied through `GRPCOptions` take precedence and control their own
refresh behavior. Channel shutdown or idleness stops the refresh timer; leaving
idle starts a new resolver. TLS and plaintext transport options are unchanged.

To query a particular DNS server directly, use a target such as
`dns://10.0.0.53:53/sink.example.com:8080`, replacing `10.0.0.53` with your DNS
server's IP. gRPC then uses Go's DNS resolver and directs queries to that server,
bypassing the operating system's native resolver cache path. The selected DNS
server, a local forwarding service, or an upstream resolver may still cache
answers. The client cannot force those servers to ignore their caches; that
requires choosing a DNS endpoint outside the cached path or changing the
DNS-server cache configuration.

During scale-in, allow enough time after endpoint removal for clients to refresh,
then gracefully drain the server. Abrupt termination can fail in-flight calls;
load balancing does not make mutating RPCs safe to replay.

The SDK interval applies to Gateway discovery; Gateway has a separate
`forwarding.dns_refresh_interval` for Engine discovery. Size each rollout's serving
overlap for endpoint publication, upstream DNS caches, refresh and lookup delays,
plus accepted requests that still retain an old Engine address snapshot.
Kubernetes `preStop` is part of the total termination grace period, whereas
Sink's `shutdown_timeout` bounds gRPC draining after SIGTERM. A refresh interval
shorter than `preStop` is not sufficient by itself. See the server's
[rollout timing and qualification guide](https://github.com/batchstream/sink/blob/main/docs/rolling-upgrades.md).

Reads retry transport-level `Unavailable` failures and retryable per-operation
failures with bounded exponential backoff and jitter. Only failed operations are
resubmitted after a partial batch response. The default is three attempts,
starting at 100 ms and capped at one second; `ClientOptions.ReadRetry` can tune
or disable retries by setting `MaxAttempts` to one.

Scan has an independent `ClientOptions.ScanRetry` policy with the same default
attempts, backoff and 20% jitter. It retries only `ResourceExhausted` statuses
carrying `google.rpc.ErrorInfo` with `domain="sink"` and
`reason="SCAN_ADMISSION_REJECTED"`, which guarantee rejection before backend
execution. Set `ScanRetry.MaxAttempts` to one to disable retries.
`ClientOptions.ScanTimeout` defaults to zero and adds no deadline. A positive value explicitly bounds the whole page,
including all attempts and backoff. A shorter caller deadline wins; cancellation
interrupts backoff. Save the next cursor only after successfully processing the
returned page; automatic admission retries do not replay successful pages.

Writes and deletes are never retried automatically. A transport error can arrive
after Sink has already applied a synchronous mutation or durably accepted an
asynchronous one, so automatic mutation retries could duplicate work. Callers
should retry only when their operation is safe under Sink's documented
at-least-once semantics.

`Read`, `Write`, and `Delete` split collections into batches of 1,000 operations
by default, matching Sink's default configuration. There are no separate `All`
variants for callers to choose between.
Set `ClientOptions.MaxOperations` when the server is configured with a different
limit. Encoded requests and responses are limited to 64 MiB by default; use
`MaxSendMessageBytes` and `MaxReceiveMessageBytes` to match custom server
limits. A `Client` and its underlying gRPC connection are safe for concurrent
use.

## Compatibility and development

Use matching SDK and server protocols. A missing requested write document is a
`ProtocolError` after the write may already have been applied, so it must not
trigger an automatic retry.

The generated protocol matches the current Sink server contract. CI
runs descriptor contract tests, race-enabled unit tests against an in-memory
gRPC server, malformed-response tests, static analysis, and an end-to-end
integration test against the matching Sink branch when available, or main, with
MongoDB and Kafka.
The compatibility workflow also runs weekly so server-side drift is detected
without requiring a client commit.

```shell
make test
make test-race
make lint
make proto-check
```

To run the external compatibility test against an already running server:

```shell
SINK_INTEGRATION_ADDRESS=127.0.0.1:8080 make test-integration
```

## Coverage regression gate

`make test-coverage` runs ordinary tests with the race detector and writes
coverage, JSON test events and a package summary to `.reports/coverage/`.
CI enforces the package floors in `.github/coverage-minimums.json`; generated
protobuf files do not count. Keep floors stable or raise them when adding tests.
The report is statement coverage, not branch or end-to-end scenario coverage.
Real backend tests remain separate from this infrastructure-free gate.
