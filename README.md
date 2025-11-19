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

Note for Google PubSub: The Google Pub/Sub publisher implementation (via Watermill/Google Cloud SDK) starts background goroutines (for batching, connection management, etc.). 
The app should call Close() to not leak 


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
  parallelism: 10  # Number of parallel subcription 

# Debugging: Enable configuration logging
log_config: false  # Set to true to log full configuration on startup
```

</details>

### Configuration Options


### Message processing parallelism

Message processing parallelism is controlled by a combination of a **broker-agnostic worker pool** and **broker-specific pull settings**.

The key concept for parallel processing is allowing a certain number of "in flight" messages from the broker. This means that the broker will not wait for a message to be acknoledge to deliver the next one, up to the specified number of allowed unacknowledge messages. This setting is specific per broker, and the library does not try to hide this settings from the user to make looking for help easier in case of errors.

After setting the maximun number of allowed "in flight" messages, further settings are used to specify the number of parallel processes handling these messages.

- **Global worker pool (`subscriber.parallelism`)**
  - This setting is needed for watermil's implementations that do not implement their own worker pool (currently RabbitMQ)
  - Controls how many **independent workers** are created per subscriber.
  - Each worker runs the handler in its own goroutine, allowing multiple messages to be processed at the same time.
  - Default value is **1** if not set.
  - Can be configured in YAML or via environment variable:
    - YAML:
      ```yaml
      subscriber:
        parallelism: 1
      ```
    - Env:
      ```bash
      export SUBSCRIBER_PARALLELISM=1
      ```

- **RabbitMQ-specific options**
  - **`broker.rabbitmq.prefetch_count`**:
    - Maximum number of unacknowledged messages that RabbitMQ will deliver **per consumer**.
    - Higher values increase throughput but also increase the number of in-flight messages and memory usage.
    - `0` means “no limit” (RabbitMQ can send as many messages as possible to each worker).
  - **`broker.rabbitmq.prefetch_size`**:
    - Byte-based limit for unacknowledged messages per consumer.
    - `0` means “no limit”; in most cases you will only tune `prefetch_count` and leave this at `0`.
  - Combined with `subscriber.parallelism`, the effective concurrency is roughly:
    - max in-flight messages ≈ `subscriber.parallelism` × `prefetch_count`
    - Example: `parallelism=5` and `prefetch_count=20` → up to ~100 messages in flight for that subscriber.

- **Google Pub/Sub-specific options**
  - ⚠️ Please note that watermill's PubSub subscriber implementation already provides a way to process messages in parallel. So there is no need to increase `subscriber.parallelism`
  - **`broker.googlepubsub.max_outstanding_messages`**:
    - Upper bound on the total number of messages being processed at once by the subscriber.
    - Acts as a backpressure mechanism; when the limit is reached, the client stops pulling new messages until some are acked.
  - **`broker.googlepubsub.num_goroutines`**:
    - Number of internal goroutines used by the Pub/Sub client to pull and dispatch messages.
    - Higher values can increase throughput on busy topics but also increase load on the broker and your application.
  - Together with `subscriber.parallelism`, these settings determine how many messages can be **pulled and processed concurrently** for each subscription.

#### `log_config` (boolean, default: `false`)

When enabled, the library will log the complete configuration (as JSON) when creating a Publisher or Subscriber. This is useful for:
- **Debugging**: Verify that configuration is loaded correctly
- **Troubleshooting**: See the actual configuration values being used (including environment variable overrides)
- **Development**: Understand how configuration precedence works

**Security Note**: Passwords in RabbitMQ URLs are automatically masked (shown as `***`) when logging.

**Example:**
```yaml
log_config: true
```

When enabled, you'll see output like:
```
=== Publisher Configuration (JSON) ===
{
  "log_config": true,
  "broker": {
    "type": "rabbitmq",
    "rabbitmq": {
      "url": "amqp://guest:***@localhost:5672/",
      ...
    }
  },
  ...
}
========================================
```

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

# Running tests in vscode

As integration tests use testcontainers, they can take a while to execute. 
If executing tests from vscode, you can specify this in your `settings.json` for the workspace

```
{
    "go.testEnvVars": {
        "TESTCONTAINERS_RYUK_DISABLED": "true"
    },
    "go.testFlags": [
        "-timeout=5m"
    ]
}
```

# Running RabbitMQ and PubSub emulator in containers

These commands can be used to run containerized versions of RabbitMQ and Google's PubSub emulator

### RabbitMQ

This exposes the administrative UI at http://localhost:8080

```
podman run -d --hostname my-rabbit --name some-rabbit -p 5672:5672 -p 8080:15672 rabbitmq:3-management
```

### Google PubSub emulator

Using the emulator also requires to set some environment variables for the Google golang Driver to use it

```
export PUBSUB_PROJECT_ID=htc-hyperfleet
export PUBSUB_EMULATOR_HOST=localhost:8085

podman run --rm --name pubsub-emulator -d -p 8085:8085 google/cloud-sdk:emulators /bin/bash -c "gcloud beta emulators pubsub start --project=hcm-hyperfleet --host-port='0.0.0.0:8085'"
```

