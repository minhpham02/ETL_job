package com.etl;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.Properties;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaFuture;

import com.etl.entities.Account;
import com.etl.entities.AccrAcctCr;
import com.etl.entities.AzAccount;
import com.etl.entities.Product;
import com.etl.entities.Teller;

public class KafkaDataProducer {
    public static void main(String[] args) {

        if (!isKafkaServerUp("192.168.26.181:9092")) {
            System.err.println("Kafka server is down. Exiting...");
            return;
        }

        // Configure producers
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.26.181:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        Properties propsProduct = new Properties();
        propsProduct.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.26.181:9092");
        propsProduct.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.LongSerializer");
        propsProduct.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        Producer<Long, String> producerProduct = new KafkaProducer<>(propsProduct);

        // Send data
        sendAccountData(producer);
        sendAccrAcctCrData(producer);
        sendAzAccount(producer);
        sendProductData(producerProduct);
        sendTellerData(producer);

        // Close producers
        producer.close();
        producerProduct.close();
    }

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

    private static void sendAccountData(Producer<String, String> producer) {
        String topic = "TRN_Account_MPC4";
        ObjectMapper mapper = new ObjectMapper();

        try {
            Account account = new Account("39", "Hacker7", "39", "USD", 5000L, new Date(System.currentTimeMillis()),
                    "A", 4500L, "1000", "1008", "001");

            String value = mapper.writeValueAsString(account);

            producer.send(new ProducerRecord<>(topic, account.getId(), value));
            System.out.println("Send Account Data" + value);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void sendAccrAcctCrData(Producer<String, String> producer) {
        String topic = "TRN_AccrAcctCr_MPC4";
        ObjectMapper mapper = new ObjectMapper();

        try {
            AccrAcctCr accrAcctCr = new AccrAcctCr("38", 38);

            String value = mapper.writeValueAsString(accrAcctCr);

            producer.send(new ProducerRecord<>(topic, accrAcctCr.getAccountNumber(), value));
            System.out.println("Send AccrAcctCr Data" + value);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void sendAzAccount(Producer<String, String> producer) {
        String topic = "TRN_AzAccount_MPC4";
        ObjectMapper mapper = new ObjectMapper();

        try {
            AzAccount azAccount = new AzAccount("38", 338);

            String value = mapper.writeValueAsString(azAccount);

            producer.send(new ProducerRecord<>(topic, azAccount.getId(), value));
            System.out.println("Send AzAccount Data" + value);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void sendProductData(Producer<Long, String> producer) {
        String topic = "TRN_product_01";
        ObjectMapper mapper = new ObjectMapper();

        try {
            Product product = new Product(
                2L,                                  // productNo: Long
                "62",                                // locTerm: String
                "Saving Account",                    // category: String
                "SubProduct2",                       // subProduct: String
                "C",                                 // opType: String
                new Date(System.currentTimeMillis()), // effectiveDate: Date
                null,                                // endDate: Date (null)
                new Timestamp(System.currentTimeMillis()) // updateTimestamp: Timestamp
            );

            String value = mapper.writeValueAsString(product);

            producer.send(new ProducerRecord<>(topic, product.getProductNo(), value));
            System.out.println("Send Product Data: " + value);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void sendTellerData(Producer<String, String> producer) {
        String topic = "TRN_Teller_MPC4";
        ObjectMapper mapper = new ObjectMapper();

        try {
            Teller teller = new Teller(
                20240415,  // VALUE_DATE_2
                "USD",                                 // CURRENCY_1
                "T1234",                               // TRANSACTION_CODE
                "1",                                   // ID
                1000.5,                                // AMOUNT_FCY_1
                300000.00,                             // AMOUNT_FCY_2
                1.25,                                  // RATE_2
                "CUST001",                             // CUSTOMER_2
                "John Doe",                            // AUTHORISER
                "A",                                   // OP_TYPE
                "ACC123",                              // ACCOUNT_1
                "ACC456"                               // ACCOUNT_2
            );

            String value = mapper.writeValueAsString(teller);

            producer.send(new ProducerRecord<>(topic, teller.getId(), value));
            System.out.println("Sent Teller Data: " + value);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}