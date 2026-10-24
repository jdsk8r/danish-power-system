package dk.powersystem.domain

import java.time.Instant

data class PowerReading(val timestamp: Instant, val measurements: Map<String, Double?>) {
    init {
        require(timestamp.epochSecond % 60 == 0L && timestamp.nano == 0) {
            "Minutes1UTC must be aligned to a full minute"
        }
        require(measurements.values.all { it == null || it.isFinite() }) {
            "Measurements must be finite or null"
        }
    }
}
