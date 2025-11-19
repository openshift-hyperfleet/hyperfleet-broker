.PHONY: build rabbitmq-up rabbitmq-down rabbitmq-logs pubsub-up pubsub-down pubsub-logs clean

# Build all binaries locally (for testing)
build:
	go build -o bin/publisher ./cmd/publisher
	go build -o bin/subscriber ./cmd/subscriber

# RabbitMQ commands
rabbitmq-up:
	cd rabbitmq && podman compose up -d

rabbitmq-down:
	cd rabbitmq && podman compose down -v

rabbitmq-logs:
	cd rabbitmq && podman compose logs -f

# Google PubSub commands
pubsub-up:
	cd googlepubsub && podman compose up -d

pubsub-down:
	cd googlepubsub && podman compose down -v

pubsub-logs:
	cd googlepubsub && podman compose logs -f

clean:
	cd rabbitmq && podman compose down -v 2>/dev/null || true
	cd googlepubsub && podman compose down -v 2>/dev/null || true
	rm -rf bin/
	rm -f broker.yaml

