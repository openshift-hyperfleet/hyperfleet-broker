# Build stage
FROM golang:1.25-alpine AS builder

WORKDIR /build

# Copy hypershift-broker directory first (needed for replace directive)
COPY . ./hyperfleet-broker/


# Download dependencies (this will use the replace directive)
WORKDIR /build/hyperfleet-broker/example
RUN go mod download

# Build binaries
RUN go build -o bin/publisher ./cmd/publisher
RUN go build -o bin/subscriber ./cmd/subscriber

# Runtime stage
FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /app

# Copy binaries from builder
COPY --from=builder /build/hyperfleet-broker/example/bin/ .

# broker.yaml will be mounted as volume in compose

CMD ["./publisher"]

