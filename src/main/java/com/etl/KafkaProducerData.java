package com.etl;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaFuture;

import com.fasterxml.jackson.databind.ObjectMapper;

public class KafkaProducerData {
    public static void main(String[] args) {
        // Kiểm tra trạng thái Kafka Server
        if (!isKafkaServerUp("192.168.26.181:9092")) {
            System.err.println("Kafka server is down. Exiting...");
            return;
        }

        // Cấu hình Kafka Producer
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.26.181:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);

        // Dữ liệu key-value
        Map<String, Object> data = new HashMap<>();
        // data.put("VALUE_DATE_2", 20250101); // valueDate2
        // data.put("CURRENCY_1", "USD");       // currency1
        // data.put("TRANSACTION_CODE", "TRX"); // transactionCode
        // data.put("ID", "7");            // id
        // data.put("AMOUNT_FCY_1", 1000.50);   // amountFcy1
        // data.put("AMOUNT_FCY_2", 2000.75);   // amountFcy2
        // data.put("RATE_2", 1.2345);          // rate2
        // data.put("CUSTOMER_2", "Cust002");   // customer2
        // data.put("AUTHORISER", "Auth");    // authoriser
        // data.put("OP_TYPE", "D");        // opType
        // data.put("ACCOUNT_1", "125");  // account1
        // data.put("ACCOUNT_2", "455");  // account2

        try {
            // Chuyển Map thành chuỗi JSON
            ObjectMapper objectMapper = new ObjectMapper();
            String jsonValue = objectMapper.writeValueAsString(data);

            // Gửi toàn bộ JSON vào Kafka
            String topic = "TRN_Teller_MPC4";
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, jsonValue);

            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    System.out.printf("Sent record with key=%s value=%s to partition=%d offset=%d%n",
                            "grouped_key_value", jsonValue, metadata.partition(), metadata.offset());
                } else {
                    exception.printStackTrace();
                }
            });

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // Đóng Producer
            producer.close();
        }
    }

    // Hàm kiểm tra trạng thái Kafka server
    private static boolean isKafkaServerUp(String bootstrapServers) {
        Properties props = new Properties();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        try (AdminClient adminClient = AdminClient.create(props)) {
            DescribeClusterResult result = adminClient.describeCluster();
            KafkaFuture<String> clusterId = result.clusterId();
            KafkaFuture<Integer> nodeCount = result.nodes().thenApply(nodes -> nodes.size());
            System.out.println("Cluster ID: " + clusterId.get());
            System.out.println("Number of nodes: " + nodeCount.get());
            System.out.println("Kafka server is up and running.");
            return true;
        } catch (Exception e) {
            System.err.println("Failed to connect to Kafka server: " + e.getMessage());
            return false;
        }
    }
}
