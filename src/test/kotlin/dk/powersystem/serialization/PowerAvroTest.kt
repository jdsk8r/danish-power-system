package dk.powersystem.serialization

import dk.powersystem.config.AppConfig
import dk.powersystem.domain.PowerReading
import dk.powersystem.domain.WindowAverage
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry
import java.nio.ByteBuffer
import java.time.Instant
import java.util.UUID
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.kafka.common.errors.SerializationException
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test

class PowerAvroTest {
    private val registryScope = "power-avro-" + UUID.randomUUID()
    private val config = AppConfig(schemaRegistryUrl = "mock://$registryScope")

    @AfterEach
    fun releaseSchemaRegistry() {
        MockSchemaRegistry.dropScope(registryScope)
    }

    @Test
    fun `readings use registered binary Avro and preserve timestamps nulls and signed values`() {
        val reading = PowerReading(
            Instant.parse("2026-10-23T12:00:00Z"),
            mapOf("SolarPower" to 0.0, "Exchange_Sum" to -42.5, "CO2Emission" to null)
        )
        PowerAvro.serde(config).use { serde ->
            val encoded = serde.serializer().serialize(config.rawTopic, PowerAvro.readingRecord(reading))
            assertThat(encoded[0]).isEqualTo(0.toByte())
            assertThat(ByteBuffer.wrap(encoded, 1, 4).int).isPositive()
            val decoded = serde.deserializer().deserialize(config.rawTopic, encoded)
            assertThat(PowerAvro.reading(decoded)).isEqualTo(reading)
            assertThat(decoded["timestamp"]).isEqualTo(reading.timestamp.toEpochMilli())
            assertThat(decoded.schema.getField("timestamp").schema().logicalType.name).isEqualTo("timestamp-millis")
            assertThat(MockSchemaRegistry.getClientForScope(registryScope).allSubjects)
                .containsExactly(config.rawTopic + "-value")
        }
    }

    @Test
    fun `averages contain only timestamp-millis window boundaries and the nullable average map`() {
        val average = WindowAverage(
            "2026-10-23T12:00:00Z", "2026-10-23T12:05:00Z",
            mapOf("SolarPower" to 50.0, "CO2Emission" to null)
        )
        PowerAvro.serde(config).use { serde ->
            val encoded = serde.serializer().serialize(config.averageTopic, PowerAvro.averageRecord(average))
            val decoded = serde.deserializer().deserialize(config.averageTopic, encoded)
            assertThat(PowerAvro.average(decoded)).isEqualTo(average)
            assertThat(decoded.schema.fields.map { it.name() })
                .containsExactly("windowStart", "windowEnd", "average")
            for (fieldName in listOf("windowStart", "windowEnd")) {
                assertThat(decoded.schema.getField(fieldName).schema().type).isEqualTo(Schema.Type.LONG)
                assertThat(decoded.schema.getField(fieldName).schema().logicalType.name).isEqualTo("timestamp-millis")
            }
            assertThat(decoded["windowStart"]).isEqualTo(Instant.parse(average.windowStart).toEpochMilli())
            assertThat(decoded["windowEnd"]).isEqualTo(Instant.parse(average.windowEnd).toEpochMilli())
            assertThat(MockSchemaRegistry.getClientForScope(registryScope).allSubjects)
                .containsExactly(config.averageTopic + "-value")
        }
    }

    @Test
    fun `new API metric names fit the existing Avro map schema`() {
        val reading = PowerReading(
            Instant.parse("2026-10-23T12:00:00Z"), mapOf("FuturePowerMetric" to 12.5)
        )
        PowerAvro.serde(config).use { serde ->
            val encoded = serde.serializer().serialize(config.rawTopic, PowerAvro.readingRecord(reading))
            assertThat(PowerAvro.reading(serde.deserializer().deserialize(config.rawTopic, encoded))).isEqualTo(reading)
        }
    }

    @Test
    fun `non-numeric measurement values fail Avro serialization`() {
        val invalidRecord = GenericData.Record(PowerAvro.readingSchema).apply {
            put("timestamp", Instant.parse("2026-10-23T12:00:00Z").toEpochMilli())
            put("measurements", mapOf("SolarPower" to "invalid"))
        }
        PowerAvro.serde(config).use { serde ->
            assertThatThrownBy {
                serde.serializer().serialize(config.rawTopic, invalidRecord)
            }.isInstanceOf(SerializationException::class.java)
        }
    }

    @Test
    fun `null values remain Kafka tombstones`() {
        PowerAvro.serde(config).use { serde ->
            assertThat(serde.serializer().serialize(config.rawTopic, null)).isNull()
            assertThat(serde.deserializer().deserialize(config.rawTopic, null)).isNull()
        }
    }
}
