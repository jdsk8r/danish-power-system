package dk.powersystem.kafka

import dk.powersystem.config.AppConfig
import dk.powersystem.domain.PowerReading
import dk.powersystem.domain.WindowAverage
import dk.powersystem.serialization.PowerAvro
import dk.powersystem.serialization.PowerJson
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.file.Path
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Properties
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.errors.GroupIdNotFoundException
import org.apache.kafka.common.errors.GroupNotEmptyException
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.CloseOptions
import org.apache.kafka.streams.KafkaStreams
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.io.TempDir

@Tag("integration")
class MultiPartitionTopologyIntegrationTest {
    @TempDir
    lateinit var stateDirectory: Path

    @Test
    @Timeout(180)
    fun `three raw partitions produce one complete average per minute`() {
        val topicPrefix = "partition-regression-" + UUID.randomUUID()
        val config = AppConfig.fromEnvironment().copy(
            rawTopic = "$topicPrefix-raw",
            averageTopic = "$topicPrefix-average",
            applicationId = "$topicPrefix-streams",
            stateDirectory = stateDirectory.toString()
        )
        Admin.create(mapOf<String, Any>(
            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to config.bootstrapServers,
            AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG to 20000,
            AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG to 10000
        )).use { admin ->
            try {
                admin.createTopics(listOf(
                    NewTopic(config.rawTopic, 3, 1.toShort()),
                    NewTopic(config.averageTopic, 3, 1.toShort())
                )).all().get(20, TimeUnit.SECONDS)
                KafkaTopics.ensureCreated(config)
                assertThat(admin.describeTopics(listOf(config.rawTopic)).allTopicNames()
                    .get(20, TimeUnit.SECONDS).getValue(config.rawTopic).partitions()).hasSize(3)

                verifyAverageAcrossPartitions(config)
            } finally {
                val temporaryTopics = admin.listTopics().names().get(20, TimeUnit.SECONDS)
                    .filter { it.startsWith("$topicPrefix-") }
                if (temporaryTopics.isNotEmpty()) {
                    admin.deleteTopics(temporaryTopics).all().get(20, TimeUnit.SECONDS)
                }
                try {
                    deleteTemporaryGroup(admin, config.applicationId)
                    deleteTemporaryGroup(admin, config.applicationId + "-verification")
                } finally {
                    deleteTemporarySchemas(config.schemaRegistryUrl, topicPrefix)
                }
            }
        }
    }

    private fun verifyAverageAcrossPartitions(config: AppConfig) {
        PowerAvro.serde(config).use { valueSerde ->
            val producerProperties = Properties().apply {
                put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers)
                put(ProducerConfig.ACKS_CONFIG, "all")
            }
            val consumerProperties = Properties().apply {
                put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers)
                put(ConsumerConfig.GROUP_ID_CONFIG, config.applicationId + "-verification")
                put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
            }
            KafkaProducer(producerProperties, StringSerializer(), valueSerde.serializer()).use { producer ->
                KafkaConsumer(consumerProperties, StringDeserializer(), valueSerde.deserializer()).use { consumer ->
                    consumer.subscribe(listOf(config.averageTopic))
                    val streams = KafkaStreams(PowerTopology.build(config), config.streamsProperties())
                    val running = CountDownLatch(1)
                    streams.setStateListener { state, _ ->
                        if (state == KafkaStreams.State.RUNNING) running.countDown()
                    }
                    try {
                        streams.start()
                        assertThat(running.await(30, TimeUnit.SECONDS)).isTrue()

                        while (Instant.now().epochSecond % 60 > 40) Thread.sleep(250)
                        val currentMinute = Instant.now().truncatedTo(ChronoUnit.MINUTES)
                        val expectedWindowEnd = currentMinute.plusSeconds(60)
                        for (minuteIndex in 0 until 5) {
                            val reading = PowerReading(
                                currentMinute.minusSeconds((4 - minuteIndex) * 60L),
                                mapOf("SolarPower" to (minuteIndex + 1) * 10.0)
                            )
                            producer.send(ProducerRecord(
                                config.rawTopic, minuteIndex % 3, reading.timestamp.toEpochMilli(),
                                PowerTopology.READING_KEY, PowerAvro.readingRecord(reading)
                            )).get(10, TimeUnit.SECONDS)
                        }

                        val observedAverages = mutableListOf<WindowAverage>()
                        val deadline = expectedWindowEnd.plusSeconds(8)
                        while (Instant.now().isBefore(deadline)) {
                            for (record in consumer.poll(Duration.ofMillis(500))) {
                                if (record.key() == expectedWindowEnd.toString()) {
                                    observedAverages.add(PowerAvro.average(record.value()))
                                }
                            }
                        }
                        assertThat(observedAverages).hasSize(1)
                        assertThat(observedAverages.single().average["SolarPower"]).isEqualTo(30.0)
                    } finally {
                        assertThat(streams.close(
                            CloseOptions.timeout(Duration.ofSeconds(10))
                                .withGroupMembershipOperation(CloseOptions.GroupMembershipOperation.LEAVE_GROUP)
                        )).isTrue()
                    }
                }
            }
        }
    }

    private fun deleteTemporaryGroup(admin: Admin, groupId: String) {
        val deadline = Instant.now().plusSeconds(15)
        while (true) {
            try {
                admin.deleteConsumerGroups(listOf(groupId)).all().get(20, TimeUnit.SECONDS)
                return
            } catch (exception: ExecutionException) {
                if (exception.cause is GroupIdNotFoundException) return
                if (exception.cause !is GroupNotEmptyException || Instant.now().isAfter(deadline)) throw exception
                Thread.sleep(250)
            }
        }
    }

    private fun deleteTemporarySchemas(registryUrl: String, topicPrefix: String) {
        val client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build()
        val baseUrl = registryUrl.trimEnd('/')
        val subjectsResponse = client.send(
            HttpRequest.newBuilder(URI.create("$baseUrl/subjects")).timeout(Duration.ofSeconds(10)).GET().build(),
            HttpResponse.BodyHandlers.ofString()
        )
        check(subjectsResponse.statusCode() == 200) { "Could not list temporary schema subjects" }
        val subjects = PowerJson.mapper.readTree(subjectsResponse.body())
            .map { it.asText() }.filter { it.startsWith("$topicPrefix-") }
        for (subject in subjects) {
            for (permanent in listOf(false, true)) {
                val deleteResponse = client.send(
                    HttpRequest.newBuilder(URI.create("$baseUrl/subjects/$subject?permanent=$permanent"))
                        .timeout(Duration.ofSeconds(10)).DELETE().build(),
                    HttpResponse.BodyHandlers.discarding()
                )
                check(deleteResponse.statusCode() == 200) { "Could not delete temporary schema subject $subject" }
            }
        }
    }
}
