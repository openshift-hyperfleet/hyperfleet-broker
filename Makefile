.PHONY: test-unit test-integration test-all

# Run unit tests in the broker folder
test-unit:
	go test -v ./broker/...

# Run tests in the test folder
test-integration:
	go test -v ./test/integration/...

# Run all tests
test-all: test-unit test-integration

