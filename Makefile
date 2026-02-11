include .bingo/Variables.mk

.PHONY: test test-integration test-all lint fmt

# Run linter
lint: $(GOLANGCI_LINT)
	$(GOLANGCI_LINT) run ./...

# Format code
fmt:
	gofmt -s -w .

# Run unit tests
test:
	go test -v ./broker/... ./pkg/... -timeout 10m

# Run tests in the test folder
test-integration:
	go test -v -p 1 ./test/integration/... -timeout 10m

# Run all tests
test-all: test test-integration

