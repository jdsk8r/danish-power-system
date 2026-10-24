package dk.powersystem.serialization

import dk.powersystem.config.AppConfig
import dk.powersystem.domain.PowerReading
import dk.powersystem.domain.WindowAverage
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde
import java.time.Instant
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord

object PowerAvro {
    val readingSchema: Schema = loadSchema("PowerSystemReading.avsc")
    val averageSchema: Schema = loadSchema("PowerSystemFiveMinuteAverage.avsc")

    fun serde(config: AppConfig): GenericAvroSerde = GenericAvroSerde().apply {
        configure(config.avroProperties(), false)
    }

    fun readingRecord(reading: PowerReading): GenericRecord = GenericData.Record(readingSchema).apply {
        put("timestamp", reading.timestamp.toEpochMilli())
        put("measurements", reading.measurements)
    }

    fun reading(record: GenericRecord): PowerReading {
        require(record.schema.fullName == readingSchema.fullName) { "Expected a PowerSystemReading record" }
        return PowerReading(timestamp(record["timestamp"]), nullableMeasurements(record["measurements"]))
    }

    fun averageRecord(average: WindowAverage): GenericRecord = GenericData.Record(averageSchema).apply {
        put("windowStart", Instant.parse(average.windowStart).toEpochMilli())
        put("windowEnd", Instant.parse(average.windowEnd).toEpochMilli())
        put("average", average.average)
    }

    fun average(record: GenericRecord): WindowAverage {
        require(record.schema.fullName == averageSchema.fullName) {
            "Expected a PowerSystemFiveMinuteAverage record"
        }
        return WindowAverage(
            timestamp(record["windowStart"]).toString(),
            timestamp(record["windowEnd"]).toString(),
            nullableMeasurements(record["average"])
        )
    }

    private fun nullableMeasurements(measurements: Any): Map<String, Double?> =
        (measurements as Map<*, *>).entries.associate { (metricName, measurement) ->
            metricName.toString() to (measurement as Number?)?.toDouble()
        }

    private fun timestamp(value: Any): Instant = when (value) {
        is Instant -> value
        is Number -> Instant.ofEpochMilli(value.toLong())
        else -> throw IllegalArgumentException("Expected an Avro timestamp-millis value")
    }

    private fun loadSchema(fileName: String): Schema =
        checkNotNull(PowerAvro::class.java.getResourceAsStream("/avro/$fileName")) {
            "Missing Avro schema resource: $fileName"
        }.use { Schema.Parser().parse(it) }
}
