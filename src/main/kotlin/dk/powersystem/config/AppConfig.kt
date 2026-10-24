package dk.powersystem.config

import java.net.URI
import java.time.Duration
import java.util.Properties
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.StreamsConfig

data class AppConfig(
    val bootstrapServers: String = "localhost:9092",
    val rawTopic: String = "app_power_system_dk_avro_v1",
    val averageTopic: String = "app_power_system_dk_5m_avg_avro_v1",
    val applicationId: String = "danish-power-system-averages-v1",
    val apiUrl: URI = URI.create("https://api.energidataservice.dk/dataset/PowerSystemRightNow"),
    val pollInterval: Duration = Duration.ofMinutes(1),
    val stateDirectory: String = ".state",
    val schemaRegistryUrl: String = "http://localhost:8081",
    val averageDecimalPlaces: Int = DEFAULT_AVERAGE_DECIMAL_PLACES
) {
    init {
        require(averageDecimalPlaces in 0..15) { "Average decimal places must be between 0 and 15" }
        require(rawTopic != averageTopic) { "Raw and average topics must differ" }
        require(pollInterval >= Duration.ofMinutes(1)) { "Poll at most once per minute" }
        require(listOf(rawTopic, averageTopic, applicationId).all {
            it.isNotBlank() && it.length <= 249 && it.matches(Regex("[a-zA-Z0-9._-]+")) &&
                it != "." && it != ".."
        }) { "Topic names and application ID must be valid Kafka names" }
        require(URI.create(schemaRegistryUrl).scheme in setOf("http", "https", "mock")) {
            "SCHEMA_REGISTRY_URL must use HTTP(S)"
        }
        require(bootstrapServers.isNotBlank()) { "KAFKA_BOOTSTRAP_SERVERS must not be blank" }
    }

    fun avroProperties(): Map<String, Any> = mapOf(
        "schema.registry.url" to schemaRegistryUrl,
        "specific.avro.reader" to false
    )

    fun streamsProperties(): Properties = Properties().apply {
        put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId)
        put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
        put(StreamsConfig.STATE_DIR_CONFIG, stateDirectory)
        put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2)
        put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1)
        put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1)
        put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000)
        put(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest")
    }

    companion object {
        const val DEFAULT_AVERAGE_DECIMAL_PLACES = 4

        fun fromEnvironment(environment: Map<String, String> = System.getenv()): AppConfig = AppConfig(
            bootstrapServers = environment["KAFKA_BOOTSTRAP_SERVERS"] ?: "localhost:9092",
            rawTopic = environment["KAFKA_RAW_TOPIC"] ?: "app_power_system_dk_avro_v1",
            averageTopic = environment["KAFKA_AVERAGE_TOPIC"] ?: "app_power_system_dk_5m_avg_avro_v1",
            applicationId = environment["KAFKA_APPLICATION_ID"] ?: "danish-power-system-averages-v1",
            apiUrl = URI.create(environment["API_URL"]
                ?: "https://api.energidataservice.dk/dataset/PowerSystemRightNow"),
            pollInterval = Duration.ofSeconds(environment["POLL_INTERVAL_SECONDS"]?.toLong() ?: 60),
            stateDirectory = environment["KAFKA_STATE_DIR"] ?: ".state",
            schemaRegistryUrl = environment["SCHEMA_REGISTRY_URL"] ?: "http://localhost:8081",
            averageDecimalPlaces = environment["AVERAGE_DECIMAL_PLACES"]?.toInt() ?: DEFAULT_AVERAGE_DECIMAL_PLACES
        )
    }
}
