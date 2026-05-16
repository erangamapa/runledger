# runledger

A ledger for agent runs. Every run my agents make ends up here as a tree of spans (the run itself, each model call, each tool call, each human approval gate) with tokens and cost attached, and a small http api answers the questions I actually ask about them.

- what did this run do, step by step, and where did the time go
- which agent or model is eating the budget this week
- how slow is p95 on the drafter since the last prompt change

Writes come in over grpc, reads go out over a Gin http api. The split is on purpose. Agents push a lot of small spans and a client stream with batching is much cheaper than an http request per span. Humans and dashboards read, and for that plain json over http is easier for everyone.

The contract is in [proto/runledger/v1/ingest.proto](proto/runledger/v1/ingest.proto), generated code is committed under `gen/`.

## Running it

```sh
export RUNLEDGER_TOKEN=dev
go run ./cmd/runledger -db dev.db
```

That serves http on `:8080` and grpc on `:9090`. Push the example run and look at it

```sh
curl -X POST -H "Authorization: Bearer dev" -H "Content-Type: application/json" \
  --data @examples/spans.json localhost:8080/v1/spans

curl -H "Authorization: Bearer dev" localhost:8080/v1/runs/run-7f3a
curl -H "Authorization: Bearer dev" "localhost:8080/v1/rollups?group=model&since=2026-10-03T00:00:00Z"
```

## Http api

| method | path | what |
| --- | --- | --- |
| GET | `/healthz` | store reachable, no auth |
| GET | `/v1/runs` | newest first, filters `agent` `tenant` `status` `since`, keyset paging with `cursor` |
| GET | `/v1/runs/:id` | run summary plus the span tree |
| GET | `/v1/rollups` | llm spans grouped by `agent`, `model` or `tenant`, last 24h by default, 31 days max |
| POST | `/v1/spans` | json ingest, up to 1000 spans per request |

Errors always look like `{"error": {"code": "...", "message": "..."}}`.

## Decisions worth knowing

Cost is computed by the ledger from [internal/pricing/prices.yaml](internal/pricing/prices.yaml), whatever the client sends is ignored. Self hosted models are priced at zero, their cost lives on the gpu bill. Money is stored as integer micro dollars because float sums over a month drift in the cents.

Span ids are unique. A retried push is safe, duplicates are counted and skipped and the run summary does not double count.

The run summary row is updated in the same transaction as the span insert, so listing runs never touches the spans table. The run span normally arrives last and is the one that sets the final status.

Spans whose parent never arrived are hung under the root in the tree view instead of vanishing. That happens more than you would think when a tool crashes before it flushes.

Bad spans in a batch are rejected one by one with a reason, the rest of the batch still lands.

## Storage

Sqlite in WAL mode through the pure Go driver, so the binary stays static and the container is distroless. For my volume (a few hundred thousand spans a day) that is plenty. The store sits behind a small interface in `internal/api` if it ever needs to move to Postgres.

## Tests

```sh
make test
```

The store is tested against a real sqlite file in a temp dir, the grpc ingest over `bufconn`, the api with `httptest`. No network.
