package dk.powersystem.ingestion

import com.sun.net.httpserver.HttpServer
import java.io.IOException
import java.net.InetSocketAddress
import java.time.Instant
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

class EnergiDataClientTest {
    @Test
    fun `client requests an overlapping interval and returns chronological readings`() {
        withServer(200, """{"records":[{"Minutes1UTC":"2026-10-23T12:02:00","SolarPower":20},{"Minutes1UTC":"2026-10-23T12:01:00","SolarPower":10}]}""") { endpoint, query ->
            val readings = EnergiDataClient(endpoint).fetchRecentReadings()
            assertThat(readings.first().timestamp).isEqualTo(Instant.parse("2026-10-23T12:01:00Z"))
            assertThat(query()).contains("start=now-PT10M", "sort=Minutes1UTC ASC")
        }
    }

    @Test
    fun `HTTP failures are surfaced for polling retries`() {
        withServer(503, "unavailable") { endpoint, _ ->
            assertThatThrownBy { EnergiDataClient(endpoint).fetchRecentReadings() }.isInstanceOf(IOException::class.java)
        }
    }

    @Test
    fun `malformed responses are rejected`() {
        withServer(200, """{"error":"no records"}""") { endpoint, _ ->
            assertThatThrownBy {
                EnergiDataClient(endpoint).fetchRecentReadings()
            }.isInstanceOf(IllegalArgumentException::class.java)
        }
    }

    private fun withServer(
        status: Int,
        body: String,
        test: (java.net.URI, () -> String) -> Unit
    ) {
        var receivedQuery = ""
        val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        server.createContext("/dataset/PowerSystemRightNow") { exchange ->
            receivedQuery = exchange.requestURI.query
            val response = body.toByteArray()
            exchange.sendResponseHeaders(status, response.size.toLong())
            exchange.responseBody.use { it.write(response) }
        }
        server.start()
        try {
            test(java.net.URI.create(
                "http://127.0.0.1:${server.address.port}/dataset/PowerSystemRightNow"
            )) { receivedQuery }
        } finally {
            server.stop(0)
        }
    }
}
