.PHONY: test test-integration test-all lint fmt

# Run linter
lint:
	golangci-lint run ./...

# Format code
fmt:
	gofmt -s -w .

# Run unit tests
test:
	go test -v ./broker/... ./pkg/... -timeout 10m

# Run tests in the test folder
test-integration:
	go test -v ./test/integration/... -timeout 10m

# Run all tests
test-all: test test-integration

