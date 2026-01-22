# Cumulocity Pulsar Microservice

A Cumulocity IoT microservice that integrates Apache Pulsar messaging with Cumulocity IoT platform. This microservice consumes messages from Apache Pulsar topics and forwards them to Cumulocity as measurements. You can use this microservice as a starting point for building custom integrations between Pulsar and Cumulocity. You can cater the input in the process_message function to your specific use case.

## Features

- Multi-tenant support for Cumulocity IoT
- Consumes messages from configurable Pulsar topics
- Sends data to Cumulocity IoT as:
  - Measurements (sensor data)

The microservice expects JSON messages with measurement data in the following format:

```json
{
  "timestamp": "2026-01-14T12:00:00Z",
  "temperature": 23.5,
  "pressure": 90
}
```

- `timestamp`: ISO 8601 formatted timestamp
- Additional fields are treated as measurement values (e.g., `temperature`, `pressure`)

## Configuration

### Environment Variables

The microservice is configured through environment variables those are set in the Cumulocity microservice environment:

C8Y_BASEURL=https://<tenant-name>.eu-latest.cumulocity.com
C8Y_BOOTSTRAP_USER=servicebootstrap_pulsar-python-mapper-ms
C8Y_BOOTSTRAP_PASSWORD=<password>
C8Y_BOOTSTRAP_TENANT=<tenant-id>
C8Y_MICROSERVICE_ISOLATION=MULTI_TENANT
C8Y_BASEURL_PULSAR=pulsar+ssl://<tenant-id>.eu-latest.cumulocity.com:6651

You can use the `create_env.sh` script to generate a `.env` file with the required environment variables for local development. Do set-sesson for the c8y CLI before running the script.

### Pulsar Authentication

The microservice uses **Cumulocity credentials** for Pulsar authentication:

## Local Development

## Deployment to Cumulocity

Use deploy.sh script to deploy the microservice to Cumulocity:

```bash

## Testing

Send a test message to mqtt-service:


```
