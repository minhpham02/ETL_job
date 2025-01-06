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
import java.sql.ResultSetMetaData;

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
                out.collect(dimAccount);
            }
            statement.close();

            if (value instanceof AccrAcctCr) {
                if (dimAccount.getRate() == null) {
                    PreparedStatement updateStatement = connection
                        .prepareStatement("UPDATE FSSTRAINING.MP_DIM_ACCOUNT SET RATE = ?, UPDATE_TMS = CURRENT_TIMESTAMP WHERE ACCOUNT_NO = ?");
                    updateStatement.setObject(1, rate);
                    updateStatement.setString(2, key);
                    updateStatement.executeUpdate();
                }
            } else if (value instanceof AzAccount) {
                if (dimAccount.getRate() == null) {
                    PreparedStatement updateStatement = connection.prepareStatement("UPDATE FSSTRAINING.MP_DIM_ACCOUNT SET RATE = ?, UPDATE_TMS = CURRENT_TIMESTAMP WHERE ACCOUNT_NO = ?");
                    updateStatement.setObject(1, rate);
                    updateStatement.setString(2, key);
                    updateStatement.executeUpdate();
                    updateStatement.close();
                }else{
                    PreparedStatement updateEndDtStatement = connection.prepareStatement("UPDATE FSSTRAINING.MP_DIM_ACCOUNT SET END_DT = CURRENT_DATE WHERE ACCOUNT_NO = ?"); 
                    updateEndDtStatement.setString(1, key); 
                    updateEndDtStatement.executeUpdate();
                    updateEndDtStatement.close();

                    PreparedStatement selectStatement = connection.prepareStatement("SELECT * FROM FSSTRAINING.MP_DIM_ACCOUNT WHERE ACCOUNT_NO = ?"); 
                    selectStatement.setString(1, key); 
                    ResultSet resultSet1 = selectStatement.executeQuery(); 
                    ResultSetMetaData metaData = resultSet.getMetaData(); 
                    int columnCount = metaData.getColumnCount(); 
                    if (resultSet1.next()) { 
                        // Tạo câu lệnh INSERT động dựa trên các cột trong bản ghi hiện tại 
                        StringBuilder columns = new StringBuilder(); 
                        StringBuilder placeholders = new StringBuilder(); 
                        for (int i = 1; i <= columnCount; i++) { 
                            String columnName = metaData.getColumnName(i); 
                            if (!columnName.equals("RATE") && !columnName.equals("UPDATE_TMS") && !columnName.equals("END_DT")) { 
                                // Bỏ qua các cột RATE, UPDATE_TMS và END_DT để cập nhật riêng 
                                if (columns.length() > 0) { 
                                    columns.append(", "); 
                                    placeholders.append(", ");
                            } 
                            columns.append(columnName); 
                            placeholders.append("?"); 
                        } 
                    } 
                    columns.append(", RATE, UPDATE_TMS"); 
                    placeholders.append(", ?, CURRENT_TIMESTAMP, CURRENT_DATE");
                     
                    String insertSql = "INSERT INTO FSSTRAINING.MP_DIM_ACCOUNT (" + columns.toString() + ") VALUES (" + placeholders.toString() + ")";
                    PreparedStatement insertStatement = connection.prepareStatement(insertSql); 
                    int index = 1; for (int i = 1; i <= columnCount; i++) { 
                        String columnName = metaData.getColumnName(i); 
                        if (!columnName.equals("RATE") && !columnName.equals("UPDATE_TMS") && !columnName.equals("END_DT")) { 
                            // Bỏ qua các cột RATE, UPDATE_TMS và END_DT để cập nhật riêng 
                            insertStatement.setObject(index++, resultSet.getObject(i)); 
                        } 
                    } 
                    insertStatement.setObject(index++, rate); 
                    insertStatement.executeUpdate(); }
                }
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
