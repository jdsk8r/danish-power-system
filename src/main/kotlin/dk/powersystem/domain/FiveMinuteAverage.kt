package dk.powersystem.domain

import java.math.BigDecimal
import java.math.RoundingMode
import java.time.Duration
import java.time.Instant

object FiveMinuteAverage {
    val windowSize: Duration = Duration.ofMinutes(5)

    fun calculate(
        windowEnd: Instant,
        readings: Collection<PowerReading>,
        decimalPlaces: Int,
        knownMetrics: Set<String> = emptySet()
    ): WindowAverage {
        require(decimalPlaces in 0..15) { "Average decimal places must be between 0 and 15" }
        val windowStart = windowEnd.minus(windowSize)
        val uniqueReadings = readings.associateBy { it.timestamp }.values.filter {
            !it.timestamp.isBefore(windowStart) && it.timestamp.isBefore(windowEnd)
        }
        val metricNames = (knownMetrics + readings.flatMap { it.measurements.keys }).toSortedSet()
        val average = metricNames.associateWith { metricName ->
            val availableValues = uniqueReadings.mapNotNull { it.measurements[metricName] }
            if (availableValues.isEmpty()) {
                null
            } else {
                val measurementSum = availableValues.fold(BigDecimal.ZERO) { totalMeasurementValue, measurementValue ->
                    totalMeasurementValue.add(BigDecimal.valueOf(measurementValue))
                }
                measurementSum.divide(
                    BigDecimal.valueOf(availableValues.size.toLong()), decimalPlaces, RoundingMode.HALF_UP
                ).toDouble()
            }
        }
        return WindowAverage(
            windowStart.toString(), windowEnd.toString(), average
        )
    }
}
