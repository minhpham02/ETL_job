package com.etl; 
// Tổ chức chương trình thành các module

import java.sql.Date;
import java.util.Properties;
// Giúp khai báo các cấu hình cho Kafka Producer

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
// ObjectMapper - 1 lớp từ thư viện Jackson (Java JSON library), dùng để chuyển đổi giữa Java Objects và JSON

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
// Đây là các lớp từ thư viện Kafka dùng để tạo producer và gửi dữ liệu đến Kafka

import com.etl.entities.Account;
// import com.etl.entities.AccrAcctCr;
// import com.etl.entities.AzAccount;
// Đây là các entity (POJO) được sử dụng để mô phỏng dữ liệu cần gửi

public class KafkaDataProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        // chứa cấu hình để kết nối và sử dụng Kafka Producer
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.26.181:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        // Value :Đây là dữ liệu chính mà bạn muốn gửi qua Kafka
        // Key :mã định danh giúp Kafka phân loại và định tuyến các bản ghi (record) đến các partition cụ thể trong topic

        // Cấu trúc dữ liệu của một bản ghi Kafka
        //     Một bản ghi Kafka được tổ chức như sau:

        //     Topic: Chủ đề của bản ghi.
        //     Partition: Phân vùng trong topic, nơi dữ liệu được lưu trữ.
        //     Key: Dùng để định tuyến bản ghi đến phân vùng (partition).
        //     Value: Dữ liệu thực sự (nội dung chính của bản ghi).
        //     Timestamp: Dấu thời gian khi bản ghi được tạo hoặc nhận.

        Producer<String,String> producer = new KafkaProducer<>(props);
        // Producer<String, String>: Producer này sẽ gửi key và value đều ở dạng String.
        // new KafkaProducer<>(props): Tạo Kafka producer với cấu hình trong props.
        
        sendAccountData(producer);
        // sendAccrAcctCrData(producer);
        // sendAzAccount(producer);
        // // Gọi 3 hàm phụ

        producer.close();
        // Đóng producer để giải phóng tài nguyên
    }

    private static void sendAccountData(Producer<String, String> producer){
        String topic = "TRN_Account_MPC01";
        // Tên topic Kafka mà dữ liệu sẽ được gửi đến
        ObjectMapper mapper = new ObjectMapper();
        // Dùng Jackson để chuyển đổi object Account thành chuỗi JSON

        // try-catch: Bắt lỗi nếu có vấn đề xảy ra trong quá trình gửi dữ liệu
        try { 
            // Account account = new Account("5", "John Doe", "32", "USD", 5000L, new Date(System.currentTimeMillis()), 
            //                                "A", 4500L, "Product123", "CategoryA", "CO001");
            Account account = new Account("7", "Michael Adam", "24", "USD", 5000L, new Date(System.currentTimeMillis()), 
                                           "A", 4500L, "Product124", "CategoryB", "CO002");
            // Account account = new Account("20", "Romeo", "ART131", "USD", 5000L, new Date(System.currentTimeMillis()), 
            //                                "C", 4500L, "Product135", "CategoryCE", "CO013");
            // Account: Một object giả định chứa thông tin tài khoản với các trường như id, name, balance, v.v.
            
            String value = mapper.writeValueAsString(account);

            producer.send(new ProducerRecord<>(topic, account.getId(), value));
            // Gửi bản ghi (ProducerRecord) chứa:
                // Topic: "TRN_Account_MPC3".
                // Key: account.getId() (mã tài khoản).
                // Value: JSON string của object Account

            System.out.println("Send Account Data" + value); //Log dữ liệu
        } catch (Exception e) {
            e.printStackTrace(); // In chi tiết lỗi để giúp debug
        }
    }



    // private static void sendAccrAcctCrData(Producer<String, String> producer){
    //     String topic = "TRN_AccrAcctCr_MPC01";
    //     ObjectMapper mapper = new ObjectMapper();

    //     try {
    //         AccrAcctCr accrAcctCr = new AccrAcctCr("5", 3);

    //         String value = mapper.writeValueAsString(accrAcctCr);

    //         producer.send(new ProducerRecord<>(topic, accrAcctCr.getAccountNumber(), value));
    //         System.out.println("Send AccrAcctCr Data" + value);
    //     } catch (Exception e) {
    //         e.printStackTrace();
    //     }
    // }



    // private static void sendAzAccount(Producer<String,String>producer){
    //     String topic = "TRN_AzAccount_MPC01";
    //     ObjectMapper mapper = new ObjectMapper();

    //     try {
    //         AzAccount AzAccount = new AzAccount("5", 3);

    //         String value = mapper.writeValueAsString(AzAccount);

    //         producer.send(new ProducerRecord<>(topic, AzAccount.getId(), value));
    //         System.out.println("Send AzAccount Data" + value);
    //     } catch (Exception e) {
    //         e.printStackTrace();
    //     }
    // }
}