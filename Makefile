include .bingo/Variables.mk

.PHONY: test test-integration test-all lint fmt gofmt go-vet install-hooks

# Run linter
lint: $(GOLANGCI_LINT)
	$(GOLANGCI_LINT) run ./...

# Format code
fmt:
	gofmt -s -w .

# Alias for fmt (required by hyperfleet-hooks)
gofmt: fmt

# Run go vet (required by hyperfleet-hooks)
go-vet:
	go vet ./...

# Install pre-commit hooks
install-hooks:
	pre-commit install

# Run unit tests
test:
	go test -v ./broker/... ./pkg/... -timeout 10m

# Run tests in the test folder (sequential packages: CI has 1 CPU so parallel execution causes timeouts)
test-integration:
	go test -v -p 1 ./test/integration/... -timeout 10m

# Run all tests
test-all: test test-integration

