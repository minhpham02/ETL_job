package com.etl.Utils;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

public class KafkaSourceUtil {
    public static <T> KafkaSource<T> createKafkaSource(String bootstrapServers, String groupId, String topic, DeserializationSchema<T> deserializationSchema) {
        return KafkaSource.<T>builder()
            .setBootstrapServers(bootstrapServers)
            .setTopics(topic)
            .setGroupId(groupId)
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(deserializationSchema)
            .build();
    }
}
