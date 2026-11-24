FROM golang:1.23 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/runledger ./cmd/runledger

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/runledger /runledger
VOLUME /data
EXPOSE 8080 9090
ENTRYPOINT ["/runledger", "-db", "/data/runledger.db"]
