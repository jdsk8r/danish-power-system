# Danish power system stream

Kafka Streams application that polls Energinet's PowerSystemRightNow dataset and computes an arithmetic average of each numeric field over the last five completed UTC minutes, every minute.

```text
Energi Data Service API
    -> HTTP poller (once per minute)
    -> app_power_system_dk_avro_v1 (raw readings)
    -> Kafka Streams (persistent minute store + wall-clock scheduler)
    -> app_power_system_dk_5m_avg_avro_v1 (averages)
```

## Code layout

Production code is grouped under `dk.powersystem` by responsibility; tests mirror the same packages.

| Package          | Responsibility                                              |
|------------------|-------------------------------------------------------------|
| `dk.powersystem` | Application entry point and lifecycle (`Main.kt`)           |
| `config`         | Environment variables and Kafka/Schema Registry settings    |
| `domain`         | Readings, window results, and five-minute averaging logic   |
| `ingestion`      | Energi Data Service client and polling producer             |
| `kafka`          | Topic setup, Streams topology, and minute-average processor |
| `serialization`  | JSON parsing and Avro record conversion                     |

Avro schemas remain under `src/main/resources/avro`.

## Requirements

- JDK 21 or newer, with `JAVA_HOME` pointing to it.
- Docker with Docker Compose, for the local Kafka broker and Schema Registry.
- Internet access for the Gradle wrapper, Maven dependencies, and Energi Data Service.

The project pins Kotlin 2.4.21, Gradle 9.8.1, Kafka Streams 4.3.1, and Confluent Avro Serdes 8.3.2. Runtime libraries include Avro 1.12.2, Jackson 2.22.3 (aligned through its BOM), and SLF4J 2.0.20. The Gradle wrapper is included; installing Gradle separately is unnecessary.

## Start Kafka

Use the [Conduktor Kafka Docker Compose stack](https://github.com/conduktor/kafka-stack-docker-compose) in a separate folder:

```text
git clone https://github.com/conduktor/kafka-stack-docker-compose.git
cd kafka-stack-docker-compose
docker compose -f full-stack.yml up -d zoo1 kafka1 kafka-schema-registry
```

Start only the three named services from the full stack. This exposes the broker at `localhost:9092` and Schema Registry at `http://localhost:8081`. Its single-broker configuration includes the transaction-topic settings needed for exactly-once stream processing. Run the application on your host machine.

Wait for Kafka and Schema Registry to finish starting, then use the included Gradle wrapper to run the `test` and `run` tasks from this project directory.

The application creates both topics with one partition and replication factor one. Existing topics may have multiple partitions. Kafka Streams combines all raw partitions through an automatically named internal topic with one partition before computing one average per minute. It also creates internal changelog topics automatically. The defaults use PLAINTEXT, suitable for this local setup.

The first average is emitted near the next UTC minute boundary after the Streams processor initializes. HTTP polling starts immediately; if data has not arrived yet, the first window can be incomplete.

## Read averages

Run this from the cloned Conduktor stack folder:

```text
docker compose -f full-stack.yml exec kafka-schema-registry kafka-avro-console-consumer --bootstrap-server kafka1:19092 --topic app_power_system_dk_5m_avg_avro_v1 --from-beginning --isolation-level read_committed --property schema.registry.url=http://kafka-schema-registry:8081 --property print.key=true --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer
```

Use `--topic app_power_system_dk_avro_v1` to inspect the raw readings.

An abbreviated average decoded by the Avro console consumer looks like:

```json
{
  "windowStart": 1792756800000,
  "windowEnd": 1792757100000,
  "average": {
    "CO2Emission": {"double": 98.2},
    "SolarPower": {"double": 1200.5},
    "Exchange_Sum": {"double": -310.4}
  }
}
```

The Kafka key is `windowEnd`, for example `2026-10-23T12:05:00Z`. The real message contains every numeric field observed, including production, solar, wind, interconnector flows, and aFRR. Averages retain each source field's unit: CO2 emission is g/kWh; power measurements are MW.

## Avro schemas

Both topics have UTF-8 string keys and Confluent-framed binary Avro values. Values carry a schema ID; schemas are registered automatically in Schema Registry under `<topic>-value`. Keys stay plain strings and do not create registry subjects.

| Topic                                | Avro record                                        | Fields                                |
|--------------------------------------|----------------------------------------------------|---------------------------------------|
| `app_power_system_dk_avro_v1`        | `dk.powersystem.avro.PowerSystemReading`           | `timestamp`, `measurements`           |
| `app_power_system_dk_5m_avg_avro_v1` | `dk.powersystem.avro.PowerSystemFiveMinuteAverage` | `windowStart`, `windowEnd`, `average` |

The schemas live in [PowerSystemReading.avsc](src/main/resources/avro/PowerSystemReading.avsc) and [PowerSystemFiveMinuteAverage.avsc](src/main/resources/avro/PowerSystemFiveMinuteAverage.avsc). They are packaged as resources and used with GenericRecord; no generated Java source is required.

- Times use Avro `long` with logical type `timestamp-millis`, representing UTC instants.
- `measurements` and `average` are maps whose values are `["null", "double"]`. Map keys preserve API metric names, such as `SolarPower` and `Exchange_Sum`. New numeric metrics fit the existing schema.
- Map fields default to `{}`. Nullable map entries represent unavailable values; an entirely missing metric may be absent.
- The Avro console consumer renders non-null union values as `{"double": value}` and timestamps as epoch-millisecond numbers.

Keep record names and field types stable. Compatible additions should have defaults and be checked against the registered schema. Use a new topic version for incompatible changes. Keep the Streams application ID stable across restarts to restore state.

## Window and delivery behavior

- At 12:05 UTC, the window contains readings timestamped 12:00 through 12:04: `[12:00, 12:05)`. At 12:06 it contains 12:01 through 12:05.
- Selection uses `Minutes1UTC`. The API omits the UTC offset in this field; the parser explicitly treats it as UTC and ignores `Minutes1DK` for aggregation.
- Averages are rounded to at most four decimal places by default. Set `AppConfig.averageDecimalPlaces` or `AVERAGE_DECIMAL_PLACES` to choose 0–15 places. Decimal arithmetic and `HALF_UP` rounding are applied to the final mean; halfway values round away from zero. Avro values remain numeric doubles, so trailing zeros are not padded.
- Each field is averaged independently. Null/missing values are excluded from its denominator. Zero and negative values are valid; missing minutes are never filled with zero.
- Polling requests the latest ten minutes in chronological order. An in-memory cache avoids republishing acknowledged, unchanged readings during a run. Failures are retried on the next poll. The ten-minute overlap recovers recent API gaps.
- Raw records use key `DK`, an event timestamp equal to `Minutes1UTC`, and an Avro record containing `timestamp` (UTC epoch milliseconds) and a nullable numeric `measurements` map. The poller can replay raw readings after a restart or an ambiguous acknowledgement.
- The Streams store identifies samples by UTC minute. Replays replace the existing sample; changed values replace the prior version. A late correction contributes to subsequent windows if it is still within their five-minute interval. Previously emitted windows are not revised.
- Scheduling uses Kafka Streams' wall clock rather than waiting for new events. A one-second scheduler checks for a new UTC minute, and emits one result even during API outages. Empty windows have null averages for previously observed metrics; before any reading is seen, the average map is empty.
- Scheduling is best effort. A paused process or broker outage can delay output. Missed minute boundaries are skipped; this application does not backfill historical averages. Very old readings are ignored, and future readings are rejected. Keep the host clock synchronized.
- Readings, known metric names, and the last emitted window are stored in Kafka-backed state stores. `exactly_once_v2` commits state, consumed offsets, and average output together. Use `read_committed` on consumers. HTTP ingestion remains at least once; the persistent minute identity prevents duplicate readings from affecting the average.
- Keep `KAFKA_APPLICATION_ID` stable across restarts to restore state. Source partitions may be consumed in parallel; one processor instance actively processes the single aggregation partition. Run one poller per raw topic.

## Configuration

Set environment variables before starting the application:

| Variable                  | Default                                                        |
|---------------------------|----------------------------------------------------------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092`                                               |
| `KAFKA_RAW_TOPIC`         | `app_power_system_dk_avro_v1`                                  |
| `KAFKA_AVERAGE_TOPIC`     | `app_power_system_dk_5m_avg_avro_v1`                           |
| `SCHEMA_REGISTRY_URL`     | `http://localhost:8081`                                        |
| `KAFKA_APPLICATION_ID`    | `danish-power-system-averages-v1`                              |
| `KAFKA_STATE_DIR`         | `.state`                                                       |
| `API_URL`                 | `https://api.energidataservice.dk/dataset/PowerSystemRightNow` |
| `POLL_INTERVAL_SECONDS`   | `60` (minimum 60)                                              |
| `AVERAGE_DECIMAL_PLACES`  | `4` (0–15)                                                     |

For example, set `KAFKA_BOOTSTRAP_SERVERS` to `localhost:9092` in your environment, then run the `run` task with the Gradle wrapper.

The optional application argument selects `all` (default), `poll`, or `streams`. Pass `run --args=poll` or `run --args=streams` to the Gradle wrapper to select a mode.

Use separate terminals when running the poller and Streams processor independently. To build a runnable distribution, run the `installDist` task with the Gradle wrapper. Start the application using the launcher for your environment in `build/install/danish-power-system/bin`.

Stop with Ctrl+C. The application stops polling, closes the producer, and closes Kafka Streams.

## Tests

`test` runs JUnit tests with AssertJ assertions for Avro wire encoding, schema registration, null handling, API parsing, UTC handling, HTTP failures, window boundaries, per-field null handling, duplicate/corrected readings, and wall-clock output with Kafka's `TopologyTestDriver`. The topology and Avro tests use an in-memory Schema Registry and do not require Docker, a broker, or an external registry.

The default suite contains 28 tests. An additional integration test creates temporary three-partition topics, verifies one complete average per minute, and removes its topics, consumer groups, and schema subjects. Run the `integrationTest` task with the Gradle wrapper while Kafka and Schema Registry are running.

It uses the configured broker and registry endpoints and takes approximately one minute. Test temporary files stay under the build directory.

## API references

- [Dataset metadata](https://api.energidataservice.dk/meta/dataset/PowerSystemRightNow)
- [Dataset page](https://www.energidataservice.dk/dataset/PowerSystemRightNow#metadata-info)
- [Data endpoint with a recent interval](https://api.energidataservice.dk/dataset/PowerSystemRightNow?start=now-PT10M)
- [Confluent Avro serialization and Schema Registry](https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/serdes-avro.html)
- [Apache Avro specification](https://avro.apache.org/docs/1.12.0/specification/)
- [Kafka Processor API and wall-clock scheduling](https://kafka.apache.org/43/streams/developer-guide/processor-api/)

Energinet publishes this dataset at one-minute resolution and recommends polling about once per minute. Its metadata identifies the measurements as real-time SCADA estimates that can contain uncorrected errors.
