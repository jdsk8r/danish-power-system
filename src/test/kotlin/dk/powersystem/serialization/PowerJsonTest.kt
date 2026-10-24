package dk.powersystem.serialization

import java.time.Instant
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

class PowerJsonTest {
    @Test
    fun `API UTC timestamps without an offset are interpreted as UTC`() {
        val reading = PowerJson.reading(
            """{"Minutes1UTC":"2026-10-25T01:00:00","Minutes1DK":"2026-10-25T02:00:00","SolarPower":0,"CO2Emission":null,"Exchange_Sum":-42.5}"""
        )
        assertThat(reading.timestamp).isEqualTo(Instant.parse("2026-10-25T01:00:00Z"))
        assertThat(reading.measurements["SolarPower"]).isEqualTo(0.0)
        assertThat(reading.measurements["Exchange_Sum"]).isEqualTo(-42.5)
        assertThat(reading.measurements["CO2Emission"]).isNull()
        assertThat(reading.measurements).doesNotContainKey("Minutes1DK")
        assertThat(PowerJson.reading(PowerJson.encode(reading))).isEqualTo(reading)
    }

    @Test
    fun `invalid records are rejected instead of being assigned the ingestion timestamp`() {
        assertThatThrownBy {
            PowerJson.reading("""{"SolarPower":1}""")
        }.isInstanceOf(IllegalArgumentException::class.java)
        assertThatThrownBy {
            PowerJson.reading("""{"Minutes1UTC":"2026-10-23T12:00:01","SolarPower":1}""")
        }.isInstanceOf(IllegalArgumentException::class.java)
        assertThatThrownBy {
            PowerJson.reading("""{"Minutes1UTC":"2026-10-23T12:00:00","SolarPower":"unknown"}""")
        }.isInstanceOf(IllegalArgumentException::class.java)
    }
}
