package dk.powersystem.ingestion

import dk.powersystem.domain.PowerReading
import dk.powersystem.serialization.PowerJson
import java.io.IOException
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration

class EnergiDataClient(
    private val endpoint: URI,
    private val httpClient: HttpClient = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(10)).build()
) {
    init {
        require(endpoint.scheme in setOf("https", "http") && endpoint.rawQuery == null) {
            "API_URL must be an HTTP(S) dataset endpoint without query parameters"
        }
    }

    fun fetchRecentReadings(): List<PowerReading> {
        val request = HttpRequest.newBuilder(
            URI.create("$endpoint?start=now-PT10M&sort=Minutes1UTC%20ASC&limit=100")
        ).timeout(Duration.ofSeconds(20))
            .header("Accept", "application/json")
            .header("User-Agent", "danish-power-system/1.0.0")
            .GET().build()
        val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
        if (response.statusCode() != 200) {
            throw IOException("Energi Data Service returned HTTP ${response.statusCode()}")
        }
        val records = PowerJson.mapper.readTree(response.body()).path("records")
        require(records.isArray) { "API response does not contain a records array" }
        return records.map(PowerJson::reading).sortedBy { it.timestamp }
    }
}
