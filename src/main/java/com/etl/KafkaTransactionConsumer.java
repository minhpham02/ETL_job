package com.etl;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import com.etl.Utils.KafkaSourceUtil;
import com.etl.entities.FactTransaction;
import com.etl.entities.Teller;
import com.fasterxml.jackson.databind.ObjectMapper;

import oracle.jdbc.pool.OracleDataSource;

public class KafkaTransactionConsumer {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String bootstrapServers = "192.168.26.181:9092";
        String groupId = "flink-consumer-group";
        ObjectMapper mapper = new ObjectMapper();

        DataStream<Teller> tellerStream = env
            .fromSource(
                KafkaSourceUtil.createKafkaSource(bootstrapServers, groupId, "TRN_Teller_MPC4",
                new SimpleStringSchema()),
                WatermarkStrategy.noWatermarks(),
                "Teller Source")
                .map(json -> mapper.readValue(json, Teller.class)); 
        
        @SuppressWarnings({ "rawtypes", "unchecked" })
        DataStream<FactTransaction> factTransaction = tellerStream
            .flatMap(new FactTransactionQueryFunction())
            .returns(FactTransaction.class);
        
        env.execute("ETL Fact Transaction");    
    }

    public static class FactTransactionQueryFunction<T> extends RichFlatMapFunction<T, FactTransaction> {
        private transient Connection connection;

        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception{
            super.open(parameters);
            OracleDataSource dataSource = new OracleDataSource();
            dataSource.setURL("jdbc:oracle:thin:@192.168.1.214:1521:dwh");
            dataSource.setUser("fsstraining");
            dataSource.setPassword("fsstraining");
            connection = dataSource.getConnection();         
        }

        @Override
        public void flatMap(T value, Collector<FactTransaction> out) throws Exception {
            // String key = "";

            // if(value instanceof Teller){
            //     Teller teller = (Teller) value;
            //     key = teller.getId();
            // }
            
            // PreparedStatement statement = connection.prepareStatement("SELECT ID FROM");
            if(value instanceof Teller){
                Teller teller = (Teller) value;
                String RecordStat = ""; 
                String OpType = teller.getOpType(); 
                String Authoriser = teller.getAuthoriser();
                String AuthStat = "";
                Date valueDate2 = null;

                if (teller.getId() == null || teller.getId().isEmpty()) { 
                    return; 
                }

                if (!"D".equals(OpType)) {
                    RecordStat = "O";
                } else if ("D".equals(OpType)) {
                    RecordStat = "C";
                }

                if (teller.getValueDate2() != null) { 
                    String valueDate2Str = teller.getValueDate2().toString(); 
                    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd"); 
                    valueDate2 = new Date(dateFormat.parse(valueDate2Str).getTime());
                }

                String currency1 = teller.getCurrency1() != null ? teller.getCurrency1() : ""; 
                String transactionCode = teller.getTransactionCode() != null ? teller.getTransactionCode() : ""; 

                BigDecimal rate = (teller.getRate2() != null) ? new BigDecimal(teller.getRate2().toString()) : BigDecimal.ONE; 
                BigDecimal amountFcy2 = (teller.getAmountFcy2() != null) ? new BigDecimal(teller.getAmountFcy2().toString()) : BigDecimal.ZERO;
                
                BigDecimal amountFcy1 = (teller.getAmountFcy1() != null) ? new BigDecimal(teller.getAmountFcy1().toString()) : BigDecimal.ZERO; 
                String customer2 = teller.getCustomer2() != null ? teller.getCustomer2() : ""; 
                
                if(OpType != null){
                    if (!"D".equals(OpType)) {
                        RecordStat = "O";
                    } else if ("D".equals(OpType)) {
                        RecordStat = "C";
                    }else{
                        RecordStat = "";
                    }
                }

                AuthStat =  (Authoriser != null) ? "A" : "U"; 
                String account1 = teller.getAccount1() != null ? teller.getAccount1() : ""; 
                String account2 = teller.getAccount2() != null ? teller.getAccount2() : "";

                String insertSql = "INSERT INTO FSSTRAINING.MP_FACT_TRANSACTION (TXN_CCY, PRODUCT_CODE, TRN_REF_NO, LCY_AMOUNT, TXN_AMOUNT, CST_DIM_ID, RECORD_STAT, AUTH_STAT, TXN_ACC, OFS_ACC, TRN_DT, ETL_DATE)" + 
                                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_DATE)";

                PreparedStatement statement = connection.prepareStatement(insertSql);
                statement.setString(1, currency1);
                statement.setString(2, transactionCode);
                statement.setString(3, teller.getId());
                statement.setBigDecimal(4, amountFcy2.multiply(rate));
                statement.setBigDecimal(5, amountFcy1);
                statement.setString(6, customer2);
                statement.setString(7, RecordStat);
                statement.setString(8, AuthStat);
                statement.setString(9, account1);
                statement.setString(10, account2);
                statement.setDate(11, valueDate2);

                System.out.println("Success");
                statement.executeUpdate();
                statement.close();
            }
        }

        @Override
        public void close() throws Exception {
            super.close();
            if (connection != null) {
                connection.close();
            }
        }
    }
}
