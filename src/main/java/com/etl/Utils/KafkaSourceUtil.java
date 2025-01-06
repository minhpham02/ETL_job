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

    // // Hàm deserialization để chuyển đổi JSON thành đối tượng
    // public static <T> T deserialize(String json, Class<T> clazz) throws IOException {
    //     ObjectMapper mapper = new ObjectMapper();
    //     return mapper.readValue(json, clazz);
    // }
    }
}
