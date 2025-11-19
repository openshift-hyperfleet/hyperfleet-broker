.PHONY: test-unit test-integration test-all

# Run unit tests in the broker folder
test-unit:
	go test -v ./broker/... -timeout 5m

# Run tests in the test folder
test-integration:
	go test -v ./test/integration/... -timeout 5m

# Run all tests
test-all: test-unit test-integration

