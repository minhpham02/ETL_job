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
        DataStream<AzAccount> azAccountStream = env
                .fromSource(
                        KafkaSourceUtil.createKafkaSource(bootstrapServers, groupId, "TRN_AzAccount_MPC4",
                                new SimpleStringSchema()),
                        WatermarkStrategy.noWatermarks(),
                        "AzAccount Source")
                .map(json -> mapper.readValue(json, AzAccount.class));

        // Kafka source cho AccrAcctCr
        DataStream<AccrAcctCr> accrAcctCrStream = env
                .fromSource(
                        KafkaSourceUtil.createKafkaSource(bootstrapServers, groupId, "TRN_AccrAcctCr_MPC4",
                                new SimpleStringSchema()),
                        WatermarkStrategy.noWatermarks(),
                        "AccrAcctCr Source")
                .map(json -> mapper.readValue(json, AccrAcctCr.class));

        // Xử lý AzAccount trước
        DataStream<DimAccount> dimAccountFromAzAccount = azAccountStream
        .flatMap(new DimAccountUpdateFunction<AzAccount>());

        // Xử lý AccrAcctCr chỉ khi InterestNumber của AzAccount là null
        DataStream<DimAccount> dimAccountFromAccrAcctCr = accrAcctCrStream
        .flatMap(new DimAccountUpdateFunction<AccrAcctCr>());

        // Kết hợp AzAccount và AccrAcctCr stream, nhưng AccrAcctCr chỉ chạy khi InterestNumber của AzAccount là null
        dimAccountFromAzAccount.union(dimAccountFromAccrAcctCr).print();

        env.execute("DimAccount Update Streaming");
    }

    public static class DimAccountUpdateFunction<T> extends RichFlatMapFunction<T, DimAccount> {
        private transient Connection connection;

        @Override
        public void open(Configuration parameters) throws Exception {
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

            // Xác định giá trị `key` và `rate` từ kiểu dữ liệu đầu vào
            if (value instanceof AzAccount) {
                AzAccount azAccount = (AzAccount) value;
                key = azAccount.getId();
                rate = azAccount.getInterestNumber();
            } else if (value instanceof AccrAcctCr) {
                AccrAcctCr accrAcctCr = (AccrAcctCr) value;
                key = accrAcctCr.getAccountNumber();
                rate = accrAcctCr.getCrIntRate();
            }

            // Query bản ghi hiện tại từ MP_DIM_ACCOUNT với điều kiện END_DT = NULL
            PreparedStatement selectStatement = connection.prepareStatement(
                    "SELECT * FROM FSSTRAINING.MP_DIM_ACCOUNT WHERE ACCOUNT_NO = ? AND END_DT IS NULL");
            selectStatement.setString(1, key);
            ResultSet resultSet = selectStatement.executeQuery();

            DimAccount dimAccount = null;
            if (resultSet.next()) {
                dimAccount = new DimAccount();
                dimAccount.setAccountNo(resultSet.getString("ACCOUNT_NO"));
                dimAccount.setRate(resultSet.getBigDecimal("RATE"));
                dimAccount.setAccountType(resultSet.getString("ACCOUNT_TYPE"));
                dimAccount.setCcy(resultSet.getString("CCY"));
                dimAccount.setCrGl(resultSet.getString("CR_GL"));
                dimAccount.setMaturityDate(resultSet.getDate("MATURITY_DATE"));
                dimAccount.setRecordStat(resultSet.getString("RECORD_STAT"));
                dimAccount.setAccountClass(resultSet.getString("ACCOUNT_CLASS"));
                dimAccount.setEffDt(resultSet.getDate("EFF_DT"));
                dimAccount.setEndDt(resultSet.getDate("END_DT"));
                dimAccount.setUpdateTms(resultSet.getTimestamp("UPDATE_TMS"));
                dimAccount.setActF(resultSet.getObject("ACT_F") != null ? (Number) resultSet.getObject("ACT_F") : null);
            }
            selectStatement.close();

            // Xử lý AzAccount
            if (value instanceof AzAccount) {
                if (dimAccount != null) {
                    if (dimAccount.getRate() == null && rate != null) {
                        // Cập nhật rate nếu rate hiện tại là null
                        updateRateForExistingAccount(key, rate);
                    } else if (dimAccount.getRate() != null) {
                        // Nếu rate cũ không null
                        if (dimAccount.getRate().equals(rate)) {
                            // Nếu rate hiện tại bằng rate cũ, bỏ qua
                            return;
                        } else {
                            // Nếu rate khác với rate cũ, cập nhật end_dt và tạo bản ghi mới
                            updateEndDtForExistingAccount(key);
                            createNewDimAccountFromOld(dimAccount, rate);
                        }
                    }
                }
            }

            // Xử lý AccrAcctCr
            if (value instanceof AccrAcctCr && dimAccount != null && dimAccount.getRate() == null) {
                // Chỉ xử lý AccrAcctCr khi InterestNumber là null
                if (rate != null) {
                    // Cập nhật rate từ CrIntRate vào bản ghi cũ
                    updateRateForExistingAccount(key, rate);
                }
            }
        }

        @Override
        public void close() throws Exception {
            if (connection != null) {
                connection.close();
            }
        }

        private void updateRateForExistingAccount(String accountNo, Number rate) throws Exception {
            String sql = "UPDATE FSSTRAINING.MP_DIM_ACCOUNT SET RATE = ?, UPDATE_TMS = CURRENT_TIMESTAMP WHERE ACCOUNT_NO = ? AND END_DT IS NULL";
            try (PreparedStatement updateStatement = connection.prepareStatement(sql)) {
                updateStatement.setObject(1, rate);
                updateStatement.setString(2, accountNo);
                int rowsAffected = updateStatement.executeUpdate();
                if (rowsAffected > 0) {
                    System.out.println("Cập nhật rate thành công cho account_no: " + accountNo);
                } else {
                    System.out.println("Không tìm thấy bản ghi với account_no: " + accountNo + " để cập nhật rate.");
                }
            }
        }

        private void updateEndDtForExistingAccount(String accountNo) throws Exception {
            String sql = "UPDATE FSSTRAINING.MP_DIM_ACCOUNT SET END_DT = CURRENT_DATE, UPDATE_TMS = CURRENT_TIMESTAMP WHERE ACCOUNT_NO = ? AND END_DT IS NULL";
            try (PreparedStatement updateEndDtStatement = connection.prepareStatement(sql)) {
                updateEndDtStatement.setString(1, accountNo);
                int rowsAffected = updateEndDtStatement.executeUpdate();
                if (rowsAffected > 0) {
                    System.out.println("Cập nhật end_dt thành công cho account_no: " + accountNo);
                } else {
                    System.out.println("Không tìm thấy bản ghi với account_no: " + accountNo + " để cập nhật end_dt.");
                }
            }
        }

        private void createNewDimAccountFromOld(DimAccount oldDimAccount, Number newRate) throws Exception {
            DimAccount newDimAccount = new DimAccount();
            newDimAccount.setAccountNo(oldDimAccount.getAccountNo());
            newDimAccount.setRate(newRate);  // Cập nhật rate mới
            newDimAccount.setAccountType(oldDimAccount.getAccountType());
            newDimAccount.setCcy(oldDimAccount.getCcy());
            newDimAccount.setCrGl(oldDimAccount.getCrGl());
            newDimAccount.setMaturityDate(oldDimAccount.getMaturityDate());
            newDimAccount.setRecordStat(oldDimAccount.getRecordStat());
            newDimAccount.setAccountClass(oldDimAccount.getAccountClass());
            newDimAccount.setEffDt(new java.sql.Date(System.currentTimeMillis()));
            newDimAccount.setEndDt(null);  // end_dt = null cho bản ghi mới
            newDimAccount.setUpdateTms(new java.sql.Timestamp(System.currentTimeMillis()));
            newDimAccount.setActF(oldDimAccount.getActF());

            // Chèn bản ghi mới vào database
            insertDimAccount(newDimAccount);
            System.out.println("Chèn bản ghi mới thành công cho account_no: " + newDimAccount.getAccountNo());
        }

        private void insertDimAccount(DimAccount dimAccount) throws Exception {
            String sql = "INSERT INTO FSSTRAINING.MP_DIM_ACCOUNT (ACCOUNT_TYPE, CCY, CR_GL, ACCOUNT_NO, MATURITY_DATE, RATE, RECORD_STAT, ACCOUNT_CLASS, EFF_DT, END_DT, UPDATE_TMS, ACT_F) "
                         + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            
            try (PreparedStatement insertStatement = connection.prepareStatement(sql)) {
                insertStatement.setString(1, dimAccount.getAccountType());
                insertStatement.setString(2, dimAccount.getCcy());
                insertStatement.setString(3, dimAccount.getCrGl());
                insertStatement.setString(4, dimAccount.getAccountNo());
                insertStatement.setDate(5, dimAccount.getMaturityDate() != null ? new java.sql.Date(dimAccount.getMaturityDate().getTime()) : null);
                insertStatement.setObject(6, dimAccount.getRate());
                insertStatement.setString(7, dimAccount.getRecordStat());
                insertStatement.setString(8, dimAccount.getAccountClass());
                insertStatement.setDate(9, dimAccount.getEffDt() != null ? new java.sql.Date(dimAccount.getEffDt().getTime()) : null);
                insertStatement.setDate(10, dimAccount.getEndDt() != null ? new java.sql.Date(dimAccount.getEndDt().getTime()) : null);
                insertStatement.setTimestamp(11, dimAccount.getUpdateTms());
                insertStatement.setString(12, dimAccount.getActF() != null ? dimAccount.getActF().toString() : null);
                insertStatement.executeUpdate();
            }
        }
    }
}
