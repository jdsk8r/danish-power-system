package dk.powersystem.domain

data class WindowAverage(
    val windowStart: String,
    val windowEnd: String,
    val average: Map<String, Double?>
)
