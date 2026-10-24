package dk.powersystem.serialization

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import dk.powersystem.domain.PowerReading
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset

object PowerJson {
    val mapper = ObjectMapper()

    fun reading(json: String): PowerReading = reading(mapper.readTree(json))

    fun reading(record: JsonNode): PowerReading {
        val utcTimestamp = record.path("Minutes1UTC")
        require(utcTimestamp.isTextual) { "Missing Minutes1UTC" }
        val timestampText = utcTimestamp.asText()
        val timestamp = if (timestampText.endsWith("Z")) {
            Instant.parse(timestampText)
        } else {
            LocalDateTime.parse(timestampText).toInstant(ZoneOffset.UTC)
        }
        val measurements = linkedMapOf<String, Double?>()
        record.properties().forEach { (fieldName, value) ->
            if (fieldName != "Minutes1UTC" && fieldName != "Minutes1DK") {
                require(value.isNumber || value.isNull) { "Non-numeric measurement: $fieldName" }
                measurements[fieldName] = if (value.isNull) null else value.doubleValue()
            }
        }
        return PowerReading(timestamp, measurements)
    }

    fun encode(reading: PowerReading): String = mapper.writeValueAsString(
        linkedMapOf<String, Any?>("Minutes1UTC" to reading.timestamp.toString()).apply {
            putAll(reading.measurements)
        }
    )
}
