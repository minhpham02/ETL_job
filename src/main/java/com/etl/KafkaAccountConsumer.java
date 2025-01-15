package com.etl;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import com.etl.entities.AccrAcctCr;
import com.etl.entities.AzAccount;
import com.etl.entities.DimAccount;

import oracle.jdbc.pool.OracleDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Timestamp;
import java.time.LocalDate;

public class KafkaAccountConsumer {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String bootstrapServers = "192.168.26.181:9092";
        String groupId = "flink-consumer-group";
        ObjectMapper mapper = new ObjectMapper();

        KafkaSource<String> kafkaAccrAcctCrSource = KafkaSource.<String>builder().setBootstrapServers(bootstrapServers)
                .setGroupId(groupId).setTopics("TRN_AccrAcctCr_MPC4").setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema()).build();

        KafkaSource<String> kafkaAzAccountSource = KafkaSource.<String>builder().setBootstrapServers(bootstrapServers)
                .setGroupId(groupId).setTopics("TRN_AzAccount_MPC4").setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema()).build();

        DataStream<AccrAcctCr> accrAcctCrStream = env
                .fromSource(
                        kafkaAccrAcctCrSource,
                        WatermarkStrategy.noWatermarks(),
                        "AccrAcctCr Source")
                .map(json -> mapper.readValue(json, AccrAcctCr.class));

        DataStream<AzAccount> azAccountStream = env
                .fromSource(
                        kafkaAzAccountSource,
                        WatermarkStrategy.noWatermarks(),
                        "AzAccount Source")
                .map(json -> mapper.readValue(json, AzAccount.class));

        @SuppressWarnings({ "unchecked", "rawtypes" })
        DataStream<DimAccount> dimAccountFromAzAccount = azAccountStream
                .flatMap(new DimAccountQueryFunction())
                .returns(DimAccount.class);

        @SuppressWarnings({ "unchecked", "rawtypes" })
        DataStream<DimAccount> dimAccountFromAccrAcctCr = accrAcctCrStream
                .flatMap(new DimAccountQueryFunction())
                .returns(DimAccount.class);

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

            // Lấy giá trị key và rate từ đối tượng value
            if (value instanceof AzAccount) {
                AzAccount azAccount = (AzAccount) value;
                key = azAccount.getId();
                rate = azAccount.getInterestNumber();
            } else if (value instanceof AccrAcctCr) {
                AccrAcctCr accrAcctCr = (AccrAcctCr) value;
                key = accrAcctCr.getAccountNumber();
                rate = accrAcctCr.getCrIntRate();
            }

            // Lấy thông tin bản ghi hiện tại từ cơ sở dữ liệu
            PreparedStatement statement = connection
                    .prepareStatement("SELECT ACCOUNT_NO, RATE FROM FSSTRAINING.MP_DIM_ACCOUNT WHERE ACCOUNT_NO = ?");
            statement.setString(1, key);
            ResultSet resultSet = statement.executeQuery();
            DimAccount dimAccount = new DimAccount();
            if (resultSet.next()) {
                dimAccount = new DimAccount();
                dimAccount.setAccountNo(resultSet.getString("ACCOUNT_NO"));
                dimAccount.setRate(resultSet.getBigDecimal("RATE"));
                out.collect(dimAccount);
            }
            statement.close();

            if (value instanceof AccrAcctCr) {
                if (dimAccount.getRate() == null) {
                    PreparedStatement updateStatement = connection.prepareStatement(
                            "UPDATE FSSTRAINING.MP_DIM_ACCOUNT SET RATE = ?, UPDATE_TMS = CURRENT_TIMESTAMP WHERE ACCOUNT_NO = ?");
                    updateStatement.setObject(1, rate);
                    updateStatement.setString(2, key);
                    updateStatement.executeUpdate();
                    updateStatement.close();
                }
            } else if (value instanceof AzAccount) {
                if (dimAccount.getRate() == null) {
                    PreparedStatement updateStatement = connection.prepareStatement(
                            "UPDATE FSSTRAINING.MP_DIM_ACCOUNT SET RATE = ?, UPDATE_TMS = CURRENT_TIMESTAMP WHERE ACCOUNT_NO = ?");
                    updateStatement.setObject(1, rate);
                    updateStatement.setString(2, key);
                    updateStatement.executeUpdate();
                    updateStatement.close();

                } else {
 
                    PreparedStatement selectStatement = connection.prepareStatement("SELECT * FROM FSSTRAINING.MP_DIM_ACCOUNT WHERE ACCOUNT_NO = ?");
                    selectStatement.setString(1, key);
                    ResultSet resultSet1 = selectStatement.executeQuery();

                    while (resultSet1.next()) {
                        LocalDate currentDate = LocalDate.now();
                        Timestamp effDtTimestamp = resultSet1.getTimestamp("EFF_DT"); 
                        LocalDate effDtLocalDate = effDtTimestamp.toLocalDateTime().toLocalDate();

                        if(!currentDate.equals(effDtLocalDate)){
                            PreparedStatement updateEndDtStatement = connection.prepareStatement(
                                "UPDATE FSSTRAINING.MP_DIM_ACCOUNT SET END_DT = CURRENT_DATE WHERE ACCOUNT_NO = ?");
                            updateEndDtStatement.setString(1, key);
                            updateEndDtStatement.executeUpdate();
                            updateEndDtStatement.close();
                                String insertSql = "INSERT INTO FSSTRAINING.MP_DIM_ACCOUNT (ACCOUNT_TYPE, CCY, CR_GL, ACCOUNT_NO, MATURITY_DATE, RATE, RECORD_STAT, ACCOUNT_CLASS, EFF_DT, END_DT, UPDATE_TMS, ACT_F)"
                                        + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_DATE, NULL, CURRENT_TIMESTAMP, ?)";
                                PreparedStatement insertStatement = connection.prepareStatement(insertSql);
                                insertStatement.setString(1, resultSet1.getString("ACCOUNT_TYPE"));
                                insertStatement.setString(2, resultSet1.getString("CCY"));
                                insertStatement.setString(3, resultSet1.getString("CR_GL"));
                                insertStatement.setString(4, resultSet1.getString("ACCOUNT_NO"));
                                insertStatement.setDate(5, resultSet1.getDate("MATURITY_DATE"));
                                insertStatement.setObject(6, rate);
                                insertStatement.setString(7, resultSet1.getString("RECORD_STAT"));
                                insertStatement.setString(8, resultSet1.getString("ACCOUNT_CLASS"));
                                insertStatement.setString(9, resultSet1.getString("ACT_F")); 

                                insertStatement.executeUpdate();
                                insertStatement.close();
                        }else{
                            PreparedStatement updateStatement = connection.prepareStatement(
                                "UPDATE FSSTRAINING.MP_DIM_ACCOUNT SET RATE = ?, UPDATE_TMS = CURRENT_TIMESTAMP WHERE ACCOUNT_NO = ? AND END_DT IS NULL");
                            updateStatement.setObject(1, rate);
                            updateStatement.setString(2, key);
                            updateStatement.executeUpdate();
                            updateStatement.close();        
                        } 
                    }
                    selectStatement.close();
                    resultSet1.close();
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
}}
