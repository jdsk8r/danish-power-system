package dk.powersystem.domain

import java.time.Instant
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource

class FiveMinuteAverageTest {
    private val windowEnd = Instant.parse("2026-10-23T12:05:00Z")

    @Test
    fun `window includes its start and excludes its end`() {
        val readings = (0..6).map { minute ->
            PowerReading(
                windowEnd.minusSeconds((6 - minute) * 60L),
                mapOf("SolarPower" to minute.toDouble())
            )
        }
        val average = FiveMinuteAverage.calculate(windowEnd, readings, decimalPlaces = 4)
        assertThat(average.average["SolarPower"]).isEqualTo(3.0)
        assertThat(average.windowStart).isEqualTo("2026-10-23T12:00:00Z")
    }

    @Test
    fun `null values are excluded for each metric and zero counts as a sample`() {
        val readings = listOf(
            PowerReading(windowEnd.minusSeconds(120), mapOf("SolarPower" to 0.0, "CO2Emission" to null)),
            PowerReading(windowEnd.minusSeconds(60), mapOf("SolarPower" to 100.0, "CO2Emission" to 30.0))
        )
        val average = FiveMinuteAverage.calculate(windowEnd, readings, decimalPlaces = 4)
        assertThat(average.average["SolarPower"]).isEqualTo(50.0)
        assertThat(average.average["CO2Emission"]).isEqualTo(30.0)
    }

    @Test
    fun `replayed minutes are replaced by the most recent version`() {
        val timestamp = windowEnd.minusSeconds(60)
        val readings = listOf(
            PowerReading(timestamp, mapOf("Exchange_Sum" to -100.0)),
            PowerReading(timestamp, mapOf("Exchange_Sum" to -200.0))
        )
        val average = FiveMinuteAverage.calculate(windowEnd, readings, decimalPlaces = 4)
        assertThat(average.average["Exchange_Sum"]).isEqualTo(-200.0)
    }

    @Test
    fun `an empty window preserves metric names without inventing zero values`() {
        val average = FiveMinuteAverage.calculate(windowEnd, emptyList(), decimalPlaces = 4, knownMetrics = setOf("SolarPower"))
        assertThat(average.average).containsEntry("SolarPower", null)
    }

    @ParameterizedTest
    @CsvSource("0, 2.0", "2, 1.67", "4, 1.6667", "6, 1.666667")
    fun `average precision is configurable`(decimalPlaces: Int, expectedAverage: Double) {
        val readings = listOf(1.0, 2.0, 2.0).mapIndexed { minuteIndex, measurement ->
            PowerReading(
                windowEnd.minusSeconds((minuteIndex + 1) * 60L), mapOf("SolarPower" to measurement)
            )
        }

        val average = FiveMinuteAverage.calculate(windowEnd, readings, decimalPlaces)

        assertThat(average.average["SolarPower"]).isEqualTo(expectedAverage)
    }

    @Test
    fun `halfway values round away from zero`() {
        val reading = PowerReading(
            windowEnd.minusSeconds(60), mapOf("SolarPower" to 1.005, "Exchange_Sum" to -1.005)
        )

        val average = FiveMinuteAverage.calculate(windowEnd, listOf(reading), decimalPlaces = 2)

        assertThat(average.average["SolarPower"]).isEqualTo(1.01)
        assertThat(average.average["Exchange_Sum"]).isEqualTo(-1.01)
    }

    @Test
    fun `only the final mean is rounded`() {
        val readings = listOf(0.00004, 0.00004, 0.00009).mapIndexed { minuteIndex, measurement ->
            PowerReading(
                windowEnd.minusSeconds((minuteIndex + 1) * 60L), mapOf("SolarPower" to measurement)
            )
        }

        val average = FiveMinuteAverage.calculate(windowEnd, readings, decimalPlaces = 4)

        assertThat(average.average["SolarPower"]).isEqualTo(0.0001)
    }
}
