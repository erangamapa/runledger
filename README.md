# runledger

A ledger for agent runs. Agents push spans over grpc, a Gin http api reads them back as run trees and cost rollups.

```sh
go run ./cmd/runledger -db dev.db
```

Contract is in [proto/runledger/v1/ingest.proto](proto/runledger/v1/ingest.proto).
