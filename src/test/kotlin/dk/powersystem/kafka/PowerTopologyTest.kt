package dk.powersystem.kafka

import dk.powersystem.config.AppConfig
import dk.powersystem.domain.PowerReading
import dk.powersystem.serialization.PowerAvro
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry
import java.nio.file.Path
import java.time.Duration
import java.time.Instant
import java.util.Properties
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.TopologyTestDriver
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.junit.jupiter.api.io.TempDir

class PowerTopologyTest {
    @TempDir
    lateinit var stateDirectory: Path

    private val registryScope = "power-topology-" + java.util.UUID.randomUUID()

    @AfterEach
    fun releaseSchemaRegistry() {
        MockSchemaRegistry.dropScope(registryScope)
    }

    @Test
    fun `wall clock emits once per UTC minute even when no new records arrive`() {
        val initialTime = Instant.parse("2026-10-23T12:05:30Z")
        val config = AppConfig(stateDirectory = stateDirectory.toString(), schemaRegistryUrl = "mock://$registryScope")
        val properties = Properties().apply {
            putAll(config.streamsProperties())
            put("processing.guarantee", "at_least_once")
        }
        TopologyTestDriver(PowerTopology.build(config), properties, initialTime).use { driver ->
            val valueSerde = PowerAvro.serde(config)
            val input = driver.createInputTopic(config.rawTopic, StringSerializer(), valueSerde.serializer())
            val output = driver.createOutputTopic(config.averageTopic, StringDeserializer(), valueSerde.deserializer())
            for (minute in 1..5) {
                val reading = PowerReading(
                    Instant.parse("2026-10-23T12:00:00Z").plusSeconds(minute * 60L),
                    mapOf("SolarPower" to minute * 10.0)
                )
                input.pipeInput(PowerTopology.READING_KEY, PowerAvro.readingRecord(reading), reading.timestamp)
                input.pipeInput(PowerTopology.READING_KEY, PowerAvro.readingRecord(reading), reading.timestamp)
            }
            assertThat(output.isEmpty).isTrue()
            driver.advanceWallClockTime(Duration.ofSeconds(30))
            val first = output.readKeyValue()
            val firstAverage = PowerAvro.average(first.value)
            assertThat(first.key).isEqualTo("2026-10-23T12:06:00Z")
            assertThat(firstAverage.average["SolarPower"]).isEqualTo(30.0)

            driver.advanceWallClockTime(Duration.ofSeconds(1))
            assertThat(output.isEmpty).isTrue()

            driver.advanceWallClockTime(Duration.ofSeconds(59))
            val nextAverage = PowerAvro.average(output.readValue())
            assertThat(nextAverage.windowEnd).isEqualTo("2026-10-23T12:07:00Z")
            assertThat(nextAverage.average["SolarPower"]).isEqualTo(35.0)
        }
    }

    @Test
    fun `corrections replace samples and stale future and null records do not inflate the window`() {
        val initialTime = Instant.parse("2026-10-23T12:05:00Z")
        val config = AppConfig(stateDirectory = stateDirectory.toString(), schemaRegistryUrl = "mock://$registryScope")
        TopologyTestDriver(PowerTopology.build(config), config.streamsProperties(), initialTime).use { driver ->
            val valueSerde = PowerAvro.serde(config)
            val input = driver.createInputTopic(config.rawTopic, StringSerializer(), valueSerde.serializer())
            val output = driver.createOutputTopic(config.averageTopic, StringDeserializer(), valueSerde.deserializer())
            fun publish(timestamp: String, measurement: Double?) {
                val reading = PowerReading(Instant.parse(timestamp), mapOf("SolarPower" to measurement))
                input.pipeInput(PowerTopology.READING_KEY, PowerAvro.readingRecord(reading), reading.timestamp)
            }
            publish("2026-10-23T11:59:00Z", 1000.0)
            publish("2026-10-23T12:06:00Z", 2000.0)
            publish("2026-10-23T12:04:00Z", 10.0)
            publish("2026-10-23T12:04:00Z", 20.0)
            publish("2026-10-23T12:05:00Z", null)
            input.pipeInput(PowerTopology.READING_KEY, null as GenericRecord?, initialTime)

            driver.advanceWallClockTime(Duration.ofMinutes(1))
            val average = PowerAvro.average(output.readValue())
            assertThat(average.average["SolarPower"]).isEqualTo(20.0)

            driver.advanceWallClockTime(Duration.ofMinutes(6))
            val emptyAverage = PowerAvro.average(output.readValue())
            assertThat(emptyAverage.average).containsEntry("SolarPower", null)
            assertThat(driver.getKeyValueStore<Long, String>(PowerTopology.READINGS_STORE)
                .get(Instant.parse("2026-10-23T12:04:00Z").toEpochMilli())).isNull()
        }
    }

    @ParameterizedTest
    @CsvSource("default, 1.6667", "2, 1.67", "0, 2.0")
    fun `configured decimal places are applied to emitted Avro averages`(
        configuredDecimalPlaces: String,
        expectedAverage: Double
    ) {
        val environment = if (configuredDecimalPlaces == "default") emptyMap() else {
            mapOf("AVERAGE_DECIMAL_PLACES" to configuredDecimalPlaces)
        }
        val config = AppConfig.fromEnvironment(environment).copy(
            stateDirectory = stateDirectory.toString(),
            schemaRegistryUrl = "mock://$registryScope"
        )
        val initialTime = Instant.parse("2026-10-23T12:05:00Z")
        TopologyTestDriver(PowerTopology.build(config), config.streamsProperties(), initialTime).use { driver ->
            val valueSerde = PowerAvro.serde(config)
            val input = driver.createInputTopic(config.rawTopic, StringSerializer(), valueSerde.serializer())
            val output = driver.createOutputTopic(
                config.averageTopic, StringDeserializer(), valueSerde.deserializer()
            )
            listOf(1.0, 2.0, 2.0).forEachIndexed { minuteIndex, measurement ->
                val reading = PowerReading(
                    initialTime.minusSeconds(minuteIndex * 60L), mapOf("SolarPower" to measurement)
                )
                input.pipeInput(PowerTopology.READING_KEY, PowerAvro.readingRecord(reading), reading.timestamp)
            }

            driver.advanceWallClockTime(Duration.ofMinutes(1))

            assertThat(PowerAvro.average(output.readValue()).average["SolarPower"]).isEqualTo(expectedAverage)
        }
    }
}
