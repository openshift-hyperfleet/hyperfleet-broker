.PHONY: test-unit test-integration test-all lint

# Run linter
lint:
	golangci-lint run ./...

# Run unit tests in the broker folder
test-unit:
	go test -v ./broker/... -timeout 10m

# Run tests in the test folder
test-integration:
	go test -v ./test/integration/... -timeout 10m

# Run all tests
test-all: test-unit test-integration

