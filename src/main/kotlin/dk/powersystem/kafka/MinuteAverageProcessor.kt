package dk.powersystem.kafka

import dk.powersystem.domain.FiveMinuteAverage
import dk.powersystem.serialization.PowerAvro
import dk.powersystem.serialization.PowerJson
import java.time.Duration
import java.time.Instant
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.streams.processor.PunctuationType
import org.apache.kafka.streams.processor.api.Processor
import org.apache.kafka.streams.processor.api.ProcessorContext
import org.apache.kafka.streams.processor.api.Record
import org.apache.kafka.streams.state.KeyValueStore
import org.slf4j.LoggerFactory

internal class MinuteAverageProcessor(
    private val averageDecimalPlaces: Int
) : Processor<String, GenericRecord, String, GenericRecord> {
    private val logger = LoggerFactory.getLogger(javaClass)
    private lateinit var context: ProcessorContext<String, GenericRecord>
    private lateinit var readingsStore: KeyValueStore<Long, String>
    private lateinit var metadataStore: KeyValueStore<String, String>
    private var startupMinuteMilliseconds = 0L
    private val minuteMilliseconds = Duration.ofMinutes(1).toMillis()

    override fun init(context: ProcessorContext<String, GenericRecord>) {
        this.context = context
        readingsStore = context.getStateStore(PowerTopology.READINGS_STORE)
        metadataStore = context.getStateStore(PowerTopology.METADATA_STORE)
        startupMinuteMilliseconds = minuteBoundary(context.currentSystemTimeMs())
        context.schedule(Duration.ofSeconds(1), PunctuationType.WALL_CLOCK_TIME, ::emitAverage)
    }

    override fun process(record: Record<String, GenericRecord>) {
        if (record.value() == null) return
        require(record.key() == PowerTopology.READING_KEY) { "Reading key must be DK" }
        val reading = PowerAvro.reading(record.value())
        val timestampMilliseconds = reading.timestamp.toEpochMilli()
        val currentMinute = minuteBoundary(context.currentSystemTimeMs())
        if (timestampMilliseconds < currentMinute - FiveMinuteAverage.windowSize.toMillis()) return
        if (timestampMilliseconds > currentMinute) {
            logger.warn("Ignoring future reading at {}", reading.timestamp)
            return
        }
        // UTC minute is the identity: overlapping polls and restarts replace, rather than add a sample.
        readingsStore.put(timestampMilliseconds, PowerJson.encode(reading))
        val knownMetrics = knownMetrics() + reading.measurements.keys
        metadataStore.put("metrics", PowerJson.mapper.writeValueAsString(knownMetrics.sorted()))
    }

    private fun emitAverage(currentTimeMilliseconds: Long) {
        val windowEndMilliseconds = minuteBoundary(currentTimeMilliseconds)
        val lastEmittedEnd = metadataStore.get("last-emitted-end")?.toLong() ?: Long.MIN_VALUE
        if (windowEndMilliseconds <= startupMinuteMilliseconds || windowEndMilliseconds <= lastEmittedEnd) return

        val windowStartMilliseconds = windowEndMilliseconds - FiveMinuteAverage.windowSize.toMillis()
        val readings = readingsStore.range(windowStartMilliseconds, windowEndMilliseconds - 1).use { entries ->
            entries.asSequence().map { PowerJson.reading(it.value) }.toList()
        }
        val average = FiveMinuteAverage.calculate(
            windowEnd = Instant.ofEpochMilli(windowEndMilliseconds),
            readings = readings,
            decimalPlaces = averageDecimalPlaces,
            knownMetrics = knownMetrics()
        )
        logger.info(
            "Publishing {} averaged power system values for the five-minute window from {} to {}",
            average.average.size, average.windowStart, average.windowEnd
        )
        context.forward(Record(
            average.windowEnd, PowerAvro.averageRecord(average), windowEndMilliseconds
        ))
        metadataStore.put("last-emitted-end", windowEndMilliseconds.toString())

        val expiredTimestamps = readingsStore.range(0L, windowStartMilliseconds - 1).use { entries ->
            entries.asSequence().map { it.key }.toList()
        }
        expiredTimestamps.forEach(readingsStore::delete)
        context.commit()
    }

    private fun knownMetrics(): Set<String> = metadataStore.get("metrics")?.let { json ->
        PowerJson.mapper.readTree(json).map { it.asText() }.toSet()
    } ?: emptySet()

    private fun minuteBoundary(timestampMilliseconds: Long): Long =
        Math.floorDiv(timestampMilliseconds, minuteMilliseconds) * minuteMilliseconds
}
