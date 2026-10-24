package dk.powersystem.config

import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

class AppConfigTest {
    @ParameterizedTest
    @ValueSource(strings = ["-1", "16", "invalid"])
    fun `invalid average decimal places are rejected`(configuredDecimalPlaces: String) {
        assertThatThrownBy {
            AppConfig.fromEnvironment(mapOf("AVERAGE_DECIMAL_PLACES" to configuredDecimalPlaces))
        }.isInstanceOf(IllegalArgumentException::class.java)
    }
}
