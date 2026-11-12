# Chapter 04 — Real-Time Data Streaming with Kafka, Spark and Delta Lake

## 1. Chapter Objective

The objective of this chapter is to build a local, real-time data streaming pipeline
using Apache Kafka, Apache Spark Structured Streaming and Delta Lake.

The pipeline receives simulated Olist order events, processes them in real time,
validates and deduplicates the events, then stores the results in a Delta Lake table.

All components are deployed locally using free and open-source technologies.

## 2. Pipeline Architecture

The pipeline follows these steps:

1. Python Producer generates simulated Olist order events in JSON format.
2. Apache Kafka receives events in the `orders` topic.
3. Spark Structured Streaming reads events continuously from Kafka.
4. Spark parses JSON messages and validates required fields.
5. Duplicate events are removed using `event_id`.
6. Delta Lake stores the processed events.

Architecture flow:

Python Producer -> Kafka Topic (orders) -> Spark Structured Streaming -> JSON Parsing -> Deduplication -> Delta Lake

## 3. Technologies Used

| Technology | Version / Configuration | Purpose |
|---|---|---|
| Python | 3.14.4 | Event generation and processing |
| Apache Kafka | 4.1.0 | Real-time event broker |
| kafka-python | 3.0.12 | Python integration with Kafka |
| Apache Spark | 4.0.2 | Structured Streaming processing |
| Delta Lake | 4.0.0 | Reliable storage and deduplication |
| Docker | 29.8.2 | Running Kafka locally |
| Docker Compose | 5.5.1 | Container management |
| WSL | Ubuntu environment | Local Linux execution |

All technologies are free to use in this local project.

## 4. Olist Event Structure

Each order event is represented as a JSON object.

| Field | Data Type | Description |
|---|---|---|
| event_id | Integer | Unique event identifier |
| order_id | String | Order identifier |
| customer_id | String | Customer identifier |
| order_status | String | Current order status |
| order_amount | Double | Order amount |
| event_timestamp | String | Event creation timestamp in UTC |

Example:

event_id: 1
order_id: OLIST_0001
customer_id: CUST_0863
order_status: created
order_amount: 45.09
event_timestamp: UTC timestamp

## 5. Python Kafka Producer

The producer is responsible for generating simulated Olist order events
and publishing them to the Kafka topic named `orders`.

Implementation file:

`streaming/kafka/producer/order_producer.py`

The producer uses the `kafka-python` library to connect to the Kafka broker
at `localhost:9092`.

Main responsibilities:
- Generate order events with unique event identifiers.
- Serialize events into JSON format.
- Publish messages to the `orders` topic.
- Confirm successful message delivery using Kafka metadata.

A separate test producer, `send_duplicate_test.py`, was also created
to publish duplicate events and test the deduplication mechanism.

## 6. Spark Structured Streaming Consumer

The Spark consumer continuously reads events from the Kafka topic `orders`.

Implementation file:

`streaming/kafka/spark/spark_kafka_consumer.py`

Main processing steps:
- Connect to the Kafka broker at `localhost:9092`.
- Read messages from the `orders` topic.
- Parse JSON messages using a predefined schema.
- Filter events with missing event identifiers or order identifiers.
- Process incoming data using `foreachBatch`.
- Remove duplicate events within each micro-batch.
- Use Delta Lake MERGE to insert only new event identifiers.

The consumer writes the processed data to:

`outputs/streaming/delta/orders`

A checkpoint directory is used to maintain streaming progress:

`outputs/streaming/checkpoints/orders`

## 7. Duplicate Event Handling

Duplicate events can occur in a streaming system.

To prevent duplicate records, the pipeline uses `event_id` as a unique key.

The deduplication strategy has two stages:

1. Within each micro-batch, Spark uses a window function and `row_number()`
   to keep only one record per `event_id`.
2. Delta Lake MERGE inserts an event only when its `event_id`
   does not already exist in the target table.

This approach provides idempotent data insertion and prevents
the same event from being stored multiple times.

The Delta table is stored at:

`outputs/streaming/delta/orders`

## 8. Streaming Data Quality and Validation

A validation script was developed:

`streaming/kafka/spark/check_streaming_data.py`

It performs five checks:

1. Verify that the Delta table is accessible.
2. Verify that all expected columns are present.
3. Verify that the table contains at least one event.
4. Verify that no duplicate `event_id` values exist.
5. Verify that required fields do not contain missing values.

### Test Results

The duplicate-event test sent event ID 12 twice to Kafka.

Results:
- Total events stored in Delta Lake: 12.
- Number of records for event ID 12: 1.
- Duplicate event identifiers: 0.
- Validation checks passed: 5/5.

All five validation tests passed successfully.

## 9. How to Run the Streaming Pipeline

All commands are executed from the project root directory.

### 1. Start Kafka

`docker start olist-kafka`

### 2. Start the Spark Streaming Consumer

`/home/user/venvs/olist-spark/bin/python streaming/kafka/spark/spark_kafka_consumer.py`

### 3. Generate Kafka Events

`streaming/kafka/.venv/bin/python streaming/kafka/producer/order_producer.py`

### 4. Validate the Delta Table

`/home/user/venvs/olist-spark/bin/python streaming/kafka/spark/check_streaming_data.py`

The consumer should be started before generating new events.

## 10. Future Improvements

The streaming pipeline can be extended with several improvements:

- Implement a controlled restart and checkpoint recovery test.
- Add real-time monitoring and alerting.
- Introduce a Dead Letter Queue for invalid events.
- Add stricter JSON schema validation.
- Handle late-arriving events using event-time processing.
- Connect the streaming outputs to a real-time dashboard.

These improvements can strengthen the reliability and scalability
of the streaming architecture.

## 11. Conclusion

This chapter implemented a local real-time streaming pipeline
using Python, Apache Kafka, Spark Structured Streaming and Delta Lake.

The pipeline successfully:
- Generated and published simulated Olist order events.
- Consumed Kafka messages using Spark Structured Streaming.
- Parsed and validated incoming JSON events.
- Prevented duplicate records using event_id and Delta MERGE.
- Stored processed events in Delta Lake.
- Passed all five streaming data quality checks.

This chapter completes the streaming component of the Olist
Data Engineering project using free and open-source technologies.
