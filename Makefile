PROTO := proto/runledger/v1/ingest.proto

.PHONY: proto test lint build run

proto:
	protoc -I proto \
		--go_out=gen --go_opt=paths=source_relative \
		--go-grpc_out=gen --go-grpc_opt=paths=source_relative \
		$(PROTO)

test:
	go test -race ./...

lint:
	go vet ./...
	test -z "$$(gofmt -l .)"

build:
	CGO_ENABLED=0 go build -o bin/runledger ./cmd/runledger

run:
	go run ./cmd/runledger -db dev.db
