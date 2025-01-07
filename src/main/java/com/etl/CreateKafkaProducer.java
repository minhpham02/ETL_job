package com.etl;

import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;

public class CreateKafkaProducer {
    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.26.181:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        AdminClient adminClient = AdminClient.create(props);
        NewTopic newTopic = new NewTopic("TRN_CompanyBranch", 10, (short) 1); 
        adminClient.createTopics(Collections.singletonList(newTopic)); adminClient.close();
    }
}