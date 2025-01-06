package com.etl;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import com.etl.Utils.KafkaSourceUtil;
import com.etl.entities.AccrAcctCr;
import com.etl.entities.AzAccount;
import com.etl.entities.DimAccount;

import oracle.jdbc.pool.OracleDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class KafkaAccountConsumer {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String bootstrapServers = "192.168.26.181:9092";
        String groupId = "flink-consumer-group";
        ObjectMapper mapper = new ObjectMapper();

        // Kafka source cho AzAccount
        DataStream<AccrAcctCr> accrAcctCrStream = env
                .fromSource(
                        KafkaSourceUtil.createKafkaSource(bootstrapServers, groupId, "TRN_AccrAcctCr_MPC4",
                                new SimpleStringSchema()),
                        WatermarkStrategy.noWatermarks(),
                        "AccrAcctCr Source")
                .map(json -> mapper.readValue(json, AccrAcctCr.class));

        // KeyedStream<AccrAcctCr, String> keyedAccrAcctCrStream =
        // accrAcctCrStream.keyBy(value -> "constant_key");
        // DataStream<AccrAcctCr> latestAccrAcctCrStream =
        // keyedAccrAcctCrStream.process(new LatestMessageProcessFunction<>());

        DataStream<AzAccount> azAccountStream = env
                .fromSource(
                        KafkaSourceUtil.createKafkaSource(bootstrapServers, groupId, "TRN_AzAccount_MPC4",
                                new SimpleStringSchema()),
                        WatermarkStrategy.noWatermarks(),
                        "AzAccount Source")
                .map(json -> mapper.readValue(json, AzAccount.class));

        // KeyedStream<AzAccount, String> keyedAzAccountStream =
        // azAccountStream.keyBy(value -> "constant_key");
        // DataStream<AzAccount> lastestAzAccount = keyedAzAccountStream.process(new
        // LatestMessageProcessFunction<>());

        @SuppressWarnings({ "unchecked", "rawtypes" })
        DataStream<DimAccount> dimAccountFromAzAccount = azAccountStream
                .flatMap(new DimAccountQueryFunction())
                .returns(DimAccount.class);

        @SuppressWarnings({ "unchecked", "rawtypes" })
        DataStream<DimAccount> dimAccountFromAccrAcctCr = accrAcctCrStream
                .flatMap(new DimAccountQueryFunction())
                .returns(DimAccount.class);

        DataStream<DimAccount> dimAccountStream = dimAccountFromAccrAcctCr.union(dimAccountFromAzAccount);

        dimAccountStream.print();

        env.execute("Filtered DimAccount Streaming");
    }

    // private static class LatestMessageProcessFunction<T> extends
    // ProcessFunction<T, T> {
    // private transient T latestMessage;

    // @Override
    // public void processElement(T value, Context ctx, Collector<T> out) {
    // latestMessage = value;
    // out.collect(latestMessage);
    // }
    // }

    public static class DimAccountQueryFunction<T> extends RichFlatMapFunction<T, DimAccount> {
        private transient Connection connection;
        // private final ObjectMapper mapper = new ObjectMapper();

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            OracleDataSource dataSource = new OracleDataSource();
            dataSource.setURL("jdbc:oracle:thin:@192.168.1.214:1521:dwh");
            dataSource.setUser("fsstraining");
            dataSource.setPassword("fsstraining");
            connection = dataSource.getConnection();
        }

        @Override
        public void flatMap(T value, Collector<DimAccount> out) throws Exception {
            String key = "";
            Number rate = null;

            if (value instanceof AzAccount) {
                AzAccount azAccount = (AzAccount) value;
                key = azAccount.getId();
                rate = azAccount.getInterestNumber();
            }

            if (value instanceof AccrAcctCr) {
                AccrAcctCr accrAcctCr = (AccrAcctCr) value;
                key = accrAcctCr.getAccountNumber();
                rate = accrAcctCr.getCrIntRate();
            }

            PreparedStatement statement = connection.prepareStatement("SELECT ACCOUNT_NO, RATE FROM FSSTRAINING.MP_DIM_ACCOUNT WHERE ACCOUNT_NO = ?");
            statement.setString(1, key);
            ResultSet resultSet = statement.executeQuery();
            DimAccount dimAccount = null;
            if (resultSet.next()) {
                dimAccount = new DimAccount();
                dimAccount.setAccountNo(resultSet.getString("ACCOUNT_NO"));
                dimAccount.setRate(resultSet.getBigDecimal("RATE"));
                System.out.println("dimAccount.getRate before update: " + dimAccount.getRate());
                out.collect(dimAccount);
            }
            statement.close();
            System.out.println("rate before update: " + rate);

            if (value instanceof AccrAcctCr) {
                if (rate != null && (dimAccount == null || dimAccount.getRate() == null)) {
                    PreparedStatement updateStatement = connection
                            .prepareStatement("UPDATE FSSTRAINING.MP_DIM_ACCOUNT SET RATE = ? WHERE ACCOUNT_NO = ?");
                    updateStatement.setObject(1, rate);
                    updateStatement.setString(2, key);
                    updateStatement.executeUpdate();
                }
            } else if (value instanceof AzAccount) {
                if (rate != null) {
                    PreparedStatement updateStatement = connection
                            .prepareStatement("UPDATE FSSTRAINING.MP_DIM_ACCOUNT SET RATE = ? WHERE ACCOUNT_NO = ?");
                    updateStatement.setObject(1, rate);
                    updateStatement.setString(2, key);
                    updateStatement.executeUpdate();
                }
            }

            if(dimAccount != null) { 
                System.out.println("dimAccount.getRate after update: " + dimAccount.getRate()); 
            } 
                System.out.println("rate after update: " + rate);
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
