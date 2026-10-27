package dk.powersystem.ingestion

import dk.powersystem.config.AppConfig
import dk.powersystem.kafka.PowerTopology
import dk.powersystem.serialization.PowerAvro
import dk.powersystem.serialization.PowerJson
import io.confluent.kafka.serializers.KafkaAvroSerializer
import java.time.Duration
import java.time.Instant
import java.util.LinkedHashMap
import java.util.Properties
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory

class ApiPoller(private val config: AppConfig) : AutoCloseable {
    private val logger = LoggerFactory.getLogger(javaClass)
    private val client = EnergiDataClient(config.apiUrl)
    private val scheduler = Executors.newSingleThreadScheduledExecutor()
    private val acknowledgedReadings = LinkedHashMap<Instant, String>()
    private val producer = KafkaProducer<String, GenericRecord>(Properties().apply {
        put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers)
        put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
        put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer::class.java.name)
        putAll(config.avroProperties())
        put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true)
        put(ProducerConfig.ACKS_CONFIG, "all")
        put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 10000)
        put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 30000)
        put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000)
    })

    fun start() {
        scheduler.scheduleWithFixedDelay(::poll, 0, config.pollInterval.toMillis(), TimeUnit.MILLISECONDS)
    }

    private fun poll() {
        try {
            val readings = client.fetchRecentReadings()
            var publishedCount = 0
            for (reading in readings) {
                val payload = PowerJson.encode(reading)
                if (acknowledgedReadings[reading.timestamp] == payload) continue
                producer.send(ProducerRecord(
                    config.rawTopic, null, reading.timestamp.toEpochMilli(), PowerTopology.READING_KEY, PowerAvro.readingRecord(reading)
                )).get(35, TimeUnit.SECONDS)
                acknowledgedReadings[reading.timestamp] = payload
                publishedCount++
            }
            val oldestRetainedTimestamp = Instant.now().minus(Duration.ofMinutes(15))
            acknowledgedReadings.keys.removeIf { it.isBefore(oldestRetainedTimestamp) }
            logger.info("Fetched {} readings; published {} new or changed readings", readings.size, publishedCount)
        } catch (_: InterruptedException) {
            Thread.currentThread().interrupt()
        } catch (exception: Exception) {
            logger.error("Polling failed; will retry on the next scheduled poll", exception)
        }
    }

    override fun close() {
        scheduler.shutdownNow()
        scheduler.awaitTermination(5, TimeUnit.SECONDS)
        producer.close(Duration.ofSeconds(5))
    }
}
