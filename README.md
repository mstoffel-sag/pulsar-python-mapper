# Cumulocity Pulsar Microservice

A Cumulocity IoT microservice that integrates Apache Pulsar messaging with Cumulocity IoT platform. This microservice consumes messages from Apache Pulsar topics and forwards them to Cumulocity as measurements, events, or alarms.

## Features

- **Event-driven architecture** using Pulsar message listener (non-blocking, efficient)
- Connects to Apache Pulsar messaging system
- Supports basic authentication using Cumulocity credentials (format: `C8Y_USER/C8Y_TENANT`)
- Consumes messages from configurable Pulsar topics
- Sends data to Cumulocity IoT as:
  - Measurements (sensor data)
  - Events (device events)
  - Alarms (alerts and warnings)
- Supports shared subscription for scalability
- Error handling and message retry logic

## Architecture

```
Apache Pulsar → [Message Listener] → Message Processing → Cumulocity IoT
```

The microservice uses an **event-driven architecture** with a Pulsar message listener:

- Messages are pushed to the listener as they arrive (no polling)
- Each message is processed asynchronously in a background thread
- Supports automatic acknowledgment and negative acknowledgment
- Efficient resource usage with no idle CPU consumption

## Message Format

The microservice expects JSON messages in the following format:

### Measurement

```json
{
  "device_id": "device123",
  "type": "measurement",
  "data": {
    "temperature": { "value": 25.5, "unit": "C" },
    "humidity": { "value": 60, "unit": "%" }
  }
}
```

### Event

```json
{
  "device_id": "device123",
  "type": "event",
  "data": {
    "type": "c8y_LocationUpdate",
    "text": "Device location updated"
  }
}
```

### Alarm

```json
{
  "device_id": "device123",
  "type": "alarm",
  "data": {
    "type": "c8y_TemperatureAlarm",
    "text": "Temperature threshold exceeded",
    "severity": "MAJOR"
  }
}
```

## Configuration

### Environment Variables

The microservice is configured through environment variables:

| Variable              | Description                                                | Default                   |
| --------------------- | ---------------------------------------------------------- | ------------------------- |
| `PULSAR_SERVICE_URL`  | Pulsar broker URL                                          | `pulsar://localhost:6650` |
| `PULSAR_SUBSCRIPTION` | Subscription name                                          | `c8y-microservice`        |
| `C8Y_BASEURL`         | Cumulocity tenant URL                                      | -                         |
| `C8Y_TENANT`          | Cumulocity tenant ID (also used for Pulsar authentication) | -                         |
| `C8Y_USER`            | Cumulocity username (also used for Pulsar authentication)  | -                         |
| `C8Y_PASSWORD`        | Cumulocity password (also used for Pulsar authentication)  | -                         |
| `LOG_LEVEL`           | Logging level (DEBUG, INFO, WARNING, ERROR)                | `INFO`                    |

### Pulsar Authentication

The microservice uses **Cumulocity credentials** for Pulsar authentication:

- **Username**: `{C8Y_USER}/{C8Y_TENANT}` (e.g., `marco.stoffel@cumulocity.com/t15264971`)
- **Password**: `{C8Y_PASSWORD}`

This eliminates the need for separate Pulsar credentials and ensures consistent authentication across both services.

### Cumulocity Microservice Settings

The microservice manifest (`cumulocity.json`) defines:

- Required permissions (inventory, measurements, events, alarms)
- Resource limits (256MB memory, 0.5 CPU)
- Configurable settings for Pulsar connection

## Local Development

### Prerequisites

- Python 3.11+
- Access to Apache Pulsar cluster
- Cumulocity IoT tenant

### Setup

1. Install dependencies:

```bash
pip install -r requirements.txt
```

2. Set environment variables:

```bash
export C8Y_BASEURL=https://your-tenant.cumulocity.com
export C8Y_TENANT=your-tenant
export C8Y_USER=your-username
export C8Y_PASSWORD=your-password
export PULSAR_SERVICE_URL=pulsar://localhost:6650
# Note: Pulsar authentication will use C8Y_USER/C8Y_TENANT and C8Y_PASSWORD
```

3. Run the microservice:

```bash
python main.py
```

## Docker Build

Build the Docker image:

```bash
docker build -t pulsar-cumulocity-microservice:1.0.0 .
```

Run locally:

```bash
docker run -e C8Y_BASEURL=<url> \
  -e C8Y_TENANT=<tenant> \
  -e C8Y_USER=<user> \
  -e C8Y_PASSWORD=<password> \
  -e PULSAR_SERVICE_URL=<pulsar-url> \
  pulsar-cumulocity-microservice:1.0.0
```

## Deployment to Cumulocity

1. Build and tag the Docker image
2. Push to a container registry accessible by Cumulocity
3. Create a ZIP file with `cumulocity.json` and docker image reference
4. Upload to Cumulocity via Administration → Applications → Add application

Or use the Cumulocity Microservice Utility:

```bash
c8y microservice create --name pulsar-bridge \
  --image your-registry/pulsar-cumulocity-microservice:1.0.0
```

## Testing

Send a test message to Pulsar:

```python
import pulsar
import json

client = pulsar.Client('pulsar://localhost:6650')
producer = client.create_producer('persistent://public/default/iot-data')

message = {
    "device_id": "test_device_001",
    "type": "measurement",
    "data": {
        "temperature": {"value": 23.5, "unit": "C"}
    }
}

producer.send(json.dumps(message).encode('utf-8'))
client.close()
```

## Monitoring

The microservice logs all activities including:

- Connection status to Pulsar and Cumulocity
- Message processing
- Errors and retries

View logs in Cumulocity: Administration → Applications → [Your Microservice] → Logs

## Required Roles

The microservice requires the following Cumulocity roles:

- `ROLE_INVENTORY_READ` - Read device inventory
- `ROLE_INVENTORY_ADMIN` - Update device inventory
- `ROLE_MEASUREMENT_ADMIN` - Create measurements
- `ROLE_EVENT_ADMIN` - Create events
- `ROLE_ALARM_ADMIN` - Create alarms

## Troubleshooting

### "Failed to decode message" Error

If you see errors like `Failed to decode JSON message: Expecting value: line 1 column 1 (char 0)`, this typically means:

1. **Empty messages**: The Pulsar topic is receiving empty messages
2. **Non-JSON format**: Messages are in binary format or not valid JSON
3. **Encoding issues**: Messages are not UTF-8 encoded

**How to debug:**

1. Enable debug logging to see raw message content:

   ```bash
   export LOG_LEVEL=DEBUG
   python main.py
   ```

2. Check the raw message format in the logs:

   ```
   DEBUG - Raw message (length=XX): b'...'
   ```

3. Verify your Pulsar messages are valid JSON:

   ```json
   {
     "device_id": "device123",
     "type": "measurement",
     "data": {...}
   }
   ```

4. If messages are in a different format (binary, Protobuf, Avro), you'll need to modify the `message_listener` method to handle that format.

### Message Listener Architecture

The microservice uses an **event-driven message listener** instead of polling. This means:

- **No timeout errors** - the listener waits passively for messages
- **More efficient** - messages are processed immediately when they arrive
- **Lower CPU usage** - no busy polling or sleep cycles
- **Better scalability** - Pulsar's internal thread pool handles concurrency

Messages are processed asynchronously in background threads managed by the Pulsar client.

### Connection Issues

- **Pulsar connection failed**: Check `PULSAR_SERVICE_URL` and network connectivity
- **Authentication failed**: Verify `C8Y_USER`, `C8Y_TENANT`, and `C8Y_PASSWORD` are correct
- **Topic not found**: Ensure the topic exists: `persistent://{tenant}/mqtt/from-device`

### Message Format

The microservice expects UTF-8 encoded JSON messages. If your Pulsar messages are in a different format:

- Binary Protocol Buffers: Add protobuf parsing
- Avro: Add Avro schema handling
- Plain text: Wrap in JSON before sending

## License

[Your License]

## Support

For issues and questions, please contact [your-support-email]
