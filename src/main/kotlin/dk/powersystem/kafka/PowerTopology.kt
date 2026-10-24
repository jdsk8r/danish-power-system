package dk.powersystem.kafka

import dk.powersystem.config.AppConfig
import dk.powersystem.serialization.PowerAvro
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Repartitioned
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.processor.api.ProcessorSupplier
import org.apache.kafka.streams.state.Stores

object PowerTopology {
    const val READINGS_STORE = "minute-readings"
    const val METADATA_STORE = "average-metadata"
    const val READING_KEY = "DK"

    fun build(config: AppConfig): Topology {
        val builder = StreamsBuilder()
        val valueSerde = PowerAvro.serde(config)
        builder.addStateStore(Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(READINGS_STORE), Serdes.Long(), Serdes.String()
        ))
        builder.addStateStore(Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(METADATA_STORE), Serdes.String(), Serdes.String()
        ))
        builder.stream(config.rawTopic, Consumed.with(Serdes.String(), valueSerde))
            .repartition(
                Repartitioned.with(Serdes.String(), valueSerde)
                    .withNumberOfPartitions(1)
            )
            .process(
                ProcessorSupplier { MinuteAverageProcessor(config.averageDecimalPlaces) },
                READINGS_STORE, METADATA_STORE
            )
            .to(config.averageTopic, Produced.with(Serdes.String(), valueSerde))
        return builder.build()
    }
}
