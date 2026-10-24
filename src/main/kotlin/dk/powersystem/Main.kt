package dk.powersystem

import dk.powersystem.config.AppConfig
import dk.powersystem.ingestion.ApiPoller
import dk.powersystem.kafka.KafkaTopics
import dk.powersystem.kafka.PowerTopology
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.system.exitProcess
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
import org.slf4j.LoggerFactory

fun main(arguments: Array<String>) {
    require(arguments.size <= 1) { "Usage: danish-power-system [all|poll|streams]" }
    val mode = arguments.firstOrNull() ?: "all"
    require(mode in setOf("all", "poll", "streams")) { "Usage: danish-power-system [all|poll|streams]" }
    val logger = LoggerFactory.getLogger("dk.powersystem.Main")
    val config = AppConfig.fromEnvironment()
    KafkaTopics.ensureCreated(config)

    val stopped = CountDownLatch(1)
    val hasFailed = AtomicBoolean(false)
    val isClosed = AtomicBoolean(false)
    val streams = if (mode != "poll") KafkaStreams(PowerTopology.build(config), config.streamsProperties()) else null
    val poller = if (mode != "streams") ApiPoller(config) else null

    fun closeApplication() {
        if (isClosed.compareAndSet(false, true)) {
            try {
                poller?.close()
            } finally {
                streams?.close(Duration.ofSeconds(10))
            }
        }
    }

    streams?.setUncaughtExceptionHandler { exception ->
        logger.error("Kafka Streams failed; stopping the application", exception)
        hasFailed.set(true)
        stopped.countDown()
        StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT
    }
    Runtime.getRuntime().addShutdownHook(Thread({
        closeApplication()
        stopped.countDown()
    }, "power-system-shutdown"))

    try {
        streams?.start()
        poller?.start()
        logger.info("Started mode={} rawTopic={} averageTopic={}", mode, config.rawTopic, config.averageTopic)
        stopped.await()
    } finally {
        closeApplication()
    }
    if (hasFailed.get()) exitProcess(1)
}
