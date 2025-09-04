# SMSGate: A Microservices-based SMS Processing Pipeline

SMSGate is an asynchronous, scalable pipeline designed to receive, parse, and store SMS messages, with a primary focus on bank transaction notifications. It leverages a microservices architecture, with communication orchestrated through a NATS messaging system and data parsing powered by Google's Gemini LLM.

## Architecture Overview

The system operates on an event-driven model where SMS messages flow through a series of specialized, independent services. This design ensures robustness, scalability, and maintainability.

* Wiki: [https://deepwiki.com/vpuhoff/smsgate](https://deepwiki.com/vpuhoff/smsgate)


**Data Flow:**

1.  **Ingestion**: The `API Gateway` receives raw SMS data via a POST request. Alternatively, the `XML Watcher` can ingest messages from XML backup files dropped into a designated folder.
2.  **Queueing**: The raw SMS is published to a NATS JetStream subject (`sms.raw`) for asynchronous processing.
3.  **Parsing**: The `Parser Worker` consumes messages from the `sms.raw` stream. It uses the Google Gemini API to intelligently parse the unstructured SMS body into a structured format (`ParsedSMS`). OTP and other non-transactional messages are filtered out.
4.  **Fanning Out**: Successfully parsed messages are published to the `sms.parsed` stream. If parsing fails, the message is sent to a dead-letter queue (`sms.failed`) for later inspection.
5.  **Persistence**: The `PocketBase Writer` service subscribes to the `sms.parsed` stream. It receives the structured data and performs an idempotent "upsert" operation into a PocketBase collection, preventing duplicate entries.

## Core Features

* **Microservices Architecture**: The system is composed of several small, independent services communicating asynchronously, making it easy to develop, deploy, and scale individual components.
* **LLM-Powered Parsing**: Utilizes Google Gemini for flexible and intelligent parsing of complex SMS formats, eliminating the need for brittle regular expressions.
* **Asynchronous & Resilient**: Built around NATS JetStream, providing durable, at-least-once message delivery and back-pressure handling.
* **Idempotent Persistence**: The database writer ensures that messages are not processed more than once, even if delivered multiple times by the message broker.
* **Observability**: Key services expose Prometheus metrics for monitoring throughput, processing time, and stream lag. It also integrates with Sentry for centralized error tracking.
* **Centralized Configuration**: All services share a unified configuration model via Pydantic settings, loading values from environment variables or a `.env` file.
* **API Caching**: Gemini API calls are cached on disk to reduce costs and improve performance for repeated messages.

## Services

The `docker-compose.yml` file orchestrates the following services:

| Service | Port(s) | Metrics Port | Description |
| :--- | :--- | :--- | :--- |
| **`nats`** | `4222` | - | The core messaging server with JetStream enabled for persistence. |
| **`api_gateway`** | `9001` | `9101` | FastAPI endpoint that ingests raw SMS messages and publishes them to the `sms.raw` stream. |
| **`parser_worker`**| - | `9102` | Consumes raw SMS, uses Gemini to parse them, and publishes results to `sms.parsed` or `sms.failed`. |
| **`pb_writer`** | - | `9103` | Subscribes to `sms.parsed` and saves the structured data to the PocketBase database. |
| **`xml_watcher`** | - | - | A fallback service that watches a directory for XML backups and ingests them into the pipeline. |
| **`nats-nui`** | `31311` | - | A web-based UI for monitoring and managing the NATS server. |

## Getting Started

### Prerequisites

* Docker & Docker Compose
* Python 3.12+ (for local development/testing)
* Access to the Google Gemini API

### Installation & Configuration

1.  **Clone the repository:**
    ```bash
    git clone <repository-url>
    cd smsgate
    ```

2.  **Create a configuration file:**
    Copy the `.env.example` to `.env` (you'll need to create this file) and populate it with your credentials and settings.

    **`.env` file:**
    ```env
    # NATS Connection
    NATS_DSN=nats://nats:4222

    # PocketBase Configuration (if used)
    PB_URL=http://pocketbase:8090
    PB_EMAIL=admin@example.com
    PB_PASSWORD=your_secure_password

    # Google Gemini API
    GEMINI_API_KEY=your_gemini_api_key

    # Sentry for Error Tracking (Optional)
SENTRY_DSN=your_sentry_dsn
ENABLE_SENTRY=false  # Set to true to enable Sentry error tracking
    ```

3.  **Build and Run the Stack:**
    Use Docker Compose to build the images and launch all the services.

    ```bash
    docker-compose up --build
    ```

## Usage

### Submitting an SMS

To submit a new SMS message for processing, send a `POST` request to the `api_gateway`:

**Endpoint**: `POST /sms/raw`
**URL**: `http://localhost:9001/sms/raw`

**Example Payload:**
```json
{
  "device_id": "android-pixel-9",
  "message": "APPROVED PURCHASE DB SALE: xxx. 29, 24 AREA,06.05.25 14:23,card ***. Amount:52.00 USD, Balance:10000.00 USD",
  "sender": "MyBank",
  "timestamp": 1718300000,
  "source": "device"
}