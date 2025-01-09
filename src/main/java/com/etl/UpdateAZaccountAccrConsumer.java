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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpdateAZaccountAccrConsumer {
    private static final Logger logger = LoggerFactory.getLogger(UpdateAZaccountAccrConsumer.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String bootstrapServers = "192.168.26.181:9092";
        String groupId = "flink-consumer-group_02";
        ObjectMapper mapper = new ObjectMapper();

        // Kafka source cho AccrAcctCr
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

        // Xử lý song song cho AzAccount
        DataStream<DimAccount> dimAccountFromAzAccount = azAccountStream
                .flatMap(new DimAccountAzAccountQueryFunction())
                .returns(DimAccount.class);

        // Xử lý song song cho AccrAcctCr
        DataStream<DimAccount> dimAccountFromAccrAcctCr = accrAcctCrStream
                .flatMap(new DimAccountAccrAcctCrQueryFunction())
                .returns(DimAccount.class);

        // Kết hợp các luồng
        DataStream<DimAccount> dimAccountStream = dimAccountFromAccrAcctCr.union(dimAccountFromAzAccount);

        // In ra thông tin sau khi cập nhật thành công
        dimAccountStream.print();

        env.execute("Filtered DimAccount Streaming");
    }

    // Xử lý song song cho AzAccount
    public static class DimAccountAzAccountQueryFunction extends RichFlatMapFunction<AzAccount, DimAccount> {
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
        public void flatMap(AzAccount azAccount, Collector<DimAccount> out) throws Exception {
            String key = azAccount.getId();
            Number rate = azAccount.getInterestNumber();

            // Kiểm tra trong bảng TRN_AZ_ACCOUNT
            PreparedStatement statement = connection.prepareStatement("SELECT ID, INTEREST_RATE FROM FSSTRAINING.TRN_AZ_ACCOUNT WHERE ID = ?");
            statement.setString(1, key);
            ResultSet resultSet = statement.executeQuery();
            if (resultSet.next()) {
                // Nếu đã tồn tại, kiểm tra giá trị INTEREST_RATE
                Number existingRate = resultSet.getBigDecimal("INTEREST_RATE");
                if (!existingRate.equals(rate)) {
                    PreparedStatement updateStatement = connection
                            .prepareStatement("UPDATE FSSTRAINING.TRN_AZ_ACCOUNT SET INTEREST_RATE = ? WHERE ID = ?");
                    updateStatement.setObject(1, rate);
                    updateStatement.setString(2, key);
                    updateStatement.executeUpdate();

                    // Log khi update thành công
                    logger.info("Update success: Updated rate in TRN_AZ_ACCOUNT for Account ID = " + key);
                }
            } else {
                // Nếu không tồn tại, insert dữ liệu mới
                PreparedStatement insertStatement = connection
                        .prepareStatement("INSERT INTO FSSTRAINING.TRN_AZ_ACCOUNT (ID, INTEREST_RATE) VALUES (?, ?)");
                insertStatement.setString(1, key);
                insertStatement.setObject(2, rate);
                insertStatement.executeUpdate();

                // Log khi insert thành công
                logger.info("Insert success: Inserted new record into TRN_AZ_ACCOUNT for Account ID = " + key);
            }

            // Trả về dữ liệu DimAccount
            DimAccount dimAccount = new DimAccount();
            dimAccount.setAccountNo(key);
            dimAccount.setRate(rate);
            out.collect(dimAccount);

            statement.close();
        }

        @Override
        public void close() throws Exception {
            super.close();
            if (connection != null) {
                connection.close();
            }
        }
    }

    // Xử lý song song cho AccrAcctCr
    public static class DimAccountAccrAcctCrQueryFunction extends RichFlatMapFunction<AccrAcctCr, DimAccount> {
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
        public void flatMap(AccrAcctCr accrAcctCr, Collector<DimAccount> out) throws Exception {
            String key = accrAcctCr.getAccountNumber();
            Number rate = accrAcctCr.getCrIntRate();

            // Kiểm tra trong bảng TRN_ACCR_ACCT_CR
            PreparedStatement statement = connection.prepareStatement("SELECT ACCOUNT_NUMBER, CR_INT_RATE FROM FSSTRAINING.TRN_ACCR_ACCT_CR WHERE ACCOUNT_NUMBER = ?");
            statement.setString(1, key);
            ResultSet resultSet = statement.executeQuery();
            if (resultSet.next()) {
                // Nếu đã tồn tại, kiểm tra giá trị CR_INT_RATE
                Number existingRate = resultSet.getBigDecimal("CR_INT_RATE");
                if (!existingRate.equals(rate)) {
                    PreparedStatement updateStatement = connection
                            .prepareStatement("UPDATE FSSTRAINING.TRN_ACCR_ACCT_CR SET CR_INT_RATE = ? WHERE ACCOUNT_NUMBER = ?");
                    updateStatement.setObject(1, rate);
                    updateStatement.setString(2, key);
                    updateStatement.executeUpdate();

                    // Log khi update thành công
                    logger.info("Update success: Updated rate in TRN_ACCR_ACCT_CR for Account Number = " + key);
                }
            } else {
                // Nếu không tồn tại, insert dữ liệu mới
                PreparedStatement insertStatement = connection
                        .prepareStatement("INSERT INTO FSSTRAINING.TRN_ACCR_ACCT_CR (ACCOUNT_NUMBER, CR_INT_RATE) VALUES (?, ?)");
                insertStatement.setString(1, key);
                insertStatement.setObject(2, rate);
                insertStatement.executeUpdate();

                // Log khi insert thành công
                logger.info("Insert success: Inserted new record into TRN_ACCR_ACCT_CR for Account Number = " + key);
            }

            // Trả về dữ liệu DimAccount
            DimAccount dimAccount = new DimAccount();
            dimAccount.setAccountNo(key);
            dimAccount.setRate(rate);
            out.collect(dimAccount);

            statement.close();
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
