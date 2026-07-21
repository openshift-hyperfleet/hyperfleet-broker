GO       := go
TOOL_MOD := tools/go.mod
gotool = "$(GO)" tool -modfile="$(TOOL_MOD)" $(1)

.PHONY: test test-integration test-all lint fmt gofmt go-vet install-hooks tools verify-tools

# Run linter
lint: ## Run golangci-lint
	$(call gotool,golangci-lint) run ./...

.PHONY: tools
tools: ## Ensure tool dependencies are up to date
	cd tools && "$(GO)" mod tidy

.PHONY: verify-tools
verify-tools: tools ## Fail in CI if tool module drifted
	@git diff --exit-code HEAD -- tools/go.mod tools/go.sum || (echo "tool modules out of date; run 'make tools'" && exit 1)

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

