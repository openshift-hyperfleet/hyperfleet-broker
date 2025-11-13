# Hyperfleet Broker Library

A Go library that provides a simplified abstraction layer over for pub/sub messaging with built-in CloudEvents support. The library supports multiple message brokers (RabbitMQ and Google Pub/Sub) through a unified API, making it easy to switch between brokers or develop broker-agnostic applications.

The current implementation uses [Watermill](https://github.com/ThreeDotsLabs/watermill), but it is abstracted from users.


## Features

- **Multiple Broker Support**: Works with RabbitMQ and Google Pub/Sub out of the box
- **CloudEvents Integration**: Built-in support for CloudEvents format with automatic conversion
- **Flexible Configuration**: YAML configuration files with environment variable overrides via Viper
- **Worker Pools**: Configurable parallel message processing for subscribers
- **Subscription Management**: Flexible subscription IDs for load balancing (shared subscriptions) or fanout (separate subscriptions)
- **Simple API**: Clean, easy-to-use interface that hides Watermill complexity

## Installation

```bash
go get github.com/openshift-hyperfleet/hyperfleet-broker
```

## Quick Start

<details>
<summary><strong>Publisher Example</strong></summary>

```go
package main

import (
    "context"
    "log"
    "time"

    cloudevents "github.com/cloudevents/sdk-go/v2"
    "github.com/cloudevents/sdk-go/v2/event"
    "github.com/openshift-hyperfleet/hyperfleet-broker/broker"
)

func main() {
    // Create publisher
    publisher, err := broker.NewPublisher()
    if err != nil {
        log.Fatalf("Failed to create publisher: %v", err)
    }
    defer publisher.Close()

    ctx := context.Background()
    topic := "example-topic"

    // Create a CloudEvent
    evt := event.New()
    evt.SetType("com.example.event.created")
    evt.SetSource("example-publisher")
    evt.SetID("event-123")
    evt.SetTime(time.Now())
    evt.SetData(cloudevents.ApplicationJSON, map[string]interface{}{
        "message": "Hello from publisher",
        "timestamp": time.Now().Format(time.RFC3339),
    })

    // Publish to topic
    if err := publisher.Publish(ctx, topic, &evt); err != nil {
        log.Printf("Error publishing event: %v", err)
    } else {
        log.Printf("Published event: %s", evt.ID())
    }
}
```

</details>

<details>
<summary><strong>Subscriber Example</strong></summary>

```go
package main

import (
    "context"
    "log"
    "os"
    "os/signal"
    "syscall"

    "github.com/cloudevents/sdk-go/v2/event"
    "github.com/openshift-hyperfleet/hyperfleet-broker/broker"
)

func main() {
    // Create subscriber with subscription ID
    // Subscribers with the same subscription ID share messages (load balancing)
    // Subscribers with different IDs receive all messages (fanout)
    subscriptionID := "shared-subscription"
    subscriber, err := broker.NewSubscriber(subscriptionID)
    if err != nil {
        log.Fatalf("Failed to create subscriber: %v", err)
    }
    defer subscriber.Close()

    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    topic := "example-topic"

    // Define handler function
    handler := func(ctx context.Context, evt *event.Event) error {
        log.Printf("Received event - ID: %s, Type: %s, Source: %s",
            evt.ID(), evt.Type(), evt.Source())

        // Extract data
        var data map[string]interface{}
        if err := evt.DataAs(&data); err == nil {
            log.Printf("Event data: %+v", data)
        }

        return nil
    }

    // Subscribe to topic
    if err := subscriber.Subscribe(ctx, topic, handler); err != nil {
        log.Fatalf("Failed to subscribe: %v", err)
    }

    // Wait for interrupt signal
    sigChan := make(chan os.Signal, 1)
    signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
    <-sigChan

    log.Printf("Shutting down subscriber...")
}
```

</details>

## Configuration

The library uses a `broker.yaml` configuration file that can be placed in:
1. The same directory as your executable
2. The current working directory
3. A path specified by the `BROKER_CONFIG_FILE` environment variable

<details>
<summary><strong>Configuration File Example (`broker.yaml`)</strong></summary>

```yaml
broker:
  # Broker type: "rabbitmq" or "googlepubsub"
  type: rabbitmq

  # RabbitMQ Configuration
  rabbitmq:
    url: amqp://guest:guest@localhost:5672/
    exchange_type: topic
    prefetch_count: 10
    publisher_confirm: false

  # Google Pub/Sub Configuration
  googlepubsub:
    project_id: my-gcp-project
    max_outstanding_messages: 100
    num_goroutines: 10

# Subscriber Configuration
subscriber:
  parallelism: 10  # Number of parallel workers
```

</details>

### Environment Variable Overrides

Any configuration value can be overridden using environment variables. Use dot notation with underscores:

```bash
export BROKER_TYPE=googlepubsub
export BROKER_GOOGLEPUBSUB_PROJECT_ID=my-project
export SUBSCRIBER_PARALLELISM=20
```

<details>
<summary><strong>Programmatic Configuration</strong></summary>

You can also provide configuration programmatically using a map:

```go
configMap := map[string]string{
    "broker.type": "rabbitmq",
    "broker.rabbitmq.url": "amqp://user:pass@localhost:5672/",
    "subscriber.parallelism": "5",
}

publisher, err := broker.NewPublisher(configMap)
subscriber, err := broker.NewSubscriber("my-subscription", configMap)
```

</details>

## Main Architectural Decisions

### 1. Watermill Abstraction

The library wraps Watermill to provide a simpler, CloudEvents-focused API. This decision:
- **Reduces complexity**: Users don't need to understand Watermill's internals
- **Provides consistency**: Same API regardless of underlying broker
- **Enables broker switching**: Change brokers by updating configuration

### 2. CloudEvents as First-Class Citizen

All messages are automatically converted to/from CloudEvents format:
- **Standardization**: Ensures compatibility with CloudEvents ecosystem
- **Metadata preservation**: CloudEvents attributes are preserved across broker boundaries
- **Type safety**: Structured event handling with CloudEvents SDK

### 3. Subscription ID Pattern

The subscription ID concept enables two messaging patterns:
- **Load Balancing**: Multiple subscribers with the same subscription ID share messages
- **Fanout**: Subscribers with different subscription IDs each receive all messages

This is implemented consistently across brokers:
- **RabbitMQ**: Queue names are `{topic}-{subscriptionId}`
- **Google Pub/Sub**: Subscription names are `{topic}-{subscriptionId}`

### 4. Worker Pool Architecture

Subscribers use a configurable worker pool for parallel message processing:
- **Throughput**: Process multiple messages concurrently
- **Backpressure**: Channel buffering prevents overwhelming workers
- **Isolation**: Each worker processes messages independently

### 5. Configuration Flexibility

Multiple configuration sources with clear precedence:
1. Programmatic map (highest priority)
2. Environment variables
3. Configuration file
4. Defaults (lowest priority)

## Docker Compose Examples

The library includes complete working examples with Docker Compose for both RabbitMQ and Google Pub/Sub.

<details>
<summary><strong>RabbitMQ Example</strong></summary>

Navigate to the RabbitMQ example directory:

```bash
cd example/rabbitmq
```

Start all services (RabbitMQ broker, publisher, and two subscribers):

```bash
docker-compose up -d
# or with podman:
podman-compose up -d
```

View logs:

```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f publisher
docker-compose logs -f subscriber1
docker-compose logs -f subscriber2
```

Access RabbitMQ Management UI:
- URL: http://localhost:15672
- Username: `guest`
- Password: `guest`

Stop services:

```bash
docker-compose down -v
```

**What it demonstrates:**
- Publisher publishes CloudEvents to `example-topic` every 2 seconds
- Two subscribers with the same subscription ID (`shared-subscription`) demonstrate load balancing
- Messages are distributed between subscribers (each message goes to only one subscriber)

</details>

<details>
<summary><strong>Google Pub/Sub Example</strong></summary>

Navigate to the Google Pub/Sub example directory:

```bash
cd example/googlepubsub
```

Start all services (Pub/Sub emulator, publisher, and two subscribers):

```bash
docker-compose up -d
# or with podman:
podman-compose up -d
```

View logs:

```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f publisher
docker-compose logs -f subscriber1
docker-compose logs -f subscriber2
```

Stop services:

```bash
docker-compose down -v
```

**What it demonstrates:**
- Publisher publishes CloudEvents to `example-topic` every 2 seconds
- Two subscribers with the same subscription ID share messages (load balancing)
- Google Pub/Sub emulator provides local development environment

</details>

### Example Configuration Files

Each example includes a `broker.yaml` file configured for that broker:

- `example/rabbitmq/broker.yaml`: RabbitMQ-specific configuration
- `example/googlepubsub/broker.yaml`: Google Pub/Sub-specific configuration

These files are mounted into the containers and used by the publisher and subscriber applications.
## References

- [Watermill Documentation](https://watermill.io/)
- [CloudEvents Specification](https://github.com/cloudevents/spec)
- [RabbitMQ Documentation](https://www.rabbitmq.com/documentation.html)
- [Google Cloud Pub/Sub Documentation](https://cloud.google.com/pubsub/docs)
