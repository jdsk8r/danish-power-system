package dk.powersystem.kafka

import dk.powersystem.config.AppConfig
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.errors.TopicExistsException
import org.slf4j.LoggerFactory

object KafkaTopics {
    private val logger = LoggerFactory.getLogger(KafkaTopics::class.java)

    fun ensureCreated(config: AppConfig) {
        Admin.create(mapOf<String, Any>(
            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to config.bootstrapServers,
            AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG to 30000
        )).use { admin ->
            val topicNames = listOf(config.rawTopic, config.averageTopic)
            val creation = admin.createTopics(topicNames.map { NewTopic(it, 1, 1.toShort()) })
            for ((_, completion) in creation.values()) {
                try {
                    completion.get(30, TimeUnit.SECONDS)
                } catch (exception: ExecutionException) {
                    if (exception.cause !is TopicExistsException) throw exception
                }
            }
            val descriptions = admin.describeTopics(topicNames).allTopicNames().get(30, TimeUnit.SECONDS)
            logger.info(
                "Raw topic {} has {} partitions; aggregation uses one internal partition",
                config.rawTopic, descriptions.getValue(config.rawTopic).partitions().size
            )
        }
    }
}
