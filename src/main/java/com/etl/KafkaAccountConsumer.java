package com.etl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import com.etl.Utils.KafkaSourceUtil;
import com.etl.entities.AccrAcctCr;
import com.etl.entities.AzAccount;
import com.etl.entities.DimAccount;

import oracle.jdbc.pool.OracleDataSource;

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

        // Kafka source cho AzAccount
        DataStream<AzAccount> azAccountStream = env
                .fromSource(
                        KafkaSourceUtil.createKafkaSource(bootstrapServers, groupId, "TRN_AzAccount_MPC4",
                                new SimpleStringSchema()),
                        WatermarkStrategy.noWatermarks(),
                        "AzAccount Source")
                .map(json -> mapper.readValue(json, AzAccount.class));

        // Chuyển đổi AzAccount và AccrAcctCr thành DimAccount
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

    public static class DimAccountQueryFunction<T> extends RichFlatMapFunction<T, DimAccount> {
        private transient Connection connection;

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

            // Truy vấn bản ghi có END_DT là null
            PreparedStatement statement = connection.prepareStatement(
                "SELECT ACCOUNT_NO, RATE, END_DT FROM FSSTRAINING.MP_DIM_ACCOUNT WHERE ACCOUNT_NO = ? AND END_DT IS NULL"
            );
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

            // Nếu bản ghi có END_DT = null và rate thay đổi hoặc chưa có giá trị, thì thực hiện update
            if (dimAccount != null && rate != null) {
                if (dimAccount.getRate() == null || !dimAccount.getRate().equals(rate)) {
                    PreparedStatement updateStatement = connection.prepareStatement(
                        "UPDATE FSSTRAINING.MP_DIM_ACCOUNT SET RATE = ? WHERE ACCOUNT_NO = ? AND END_DT IS NULL"
                    );
                    updateStatement.setObject(1, rate);
                    updateStatement.setString(2, key);
                    int rowsUpdated = updateStatement.executeUpdate();
                    if (rowsUpdated > 0) {
                        System.out.println("Updated rate for ACCOUNT_NO: " + key);
                    }
                    updateStatement.close();
                }
            } else {
                System.out.println("No record found or END_DT is not null for ACCOUNT_NO: " + key);
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
