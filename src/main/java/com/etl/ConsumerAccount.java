package com.etl;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;
import java.util.Objects;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import com.etl.Utils.CustomSqlSink;
import com.etl.entities.Account;
import com.etl.entities.DimAccount;

public class ConsumerAccount {

    public static void main(String[] args) throws Exception {
        // Set up Flink environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String bootstrapServers = "192.168.26.181:9092";
        String groupId = "flink-consumer-group_01";
        ObjectMapper mapper = new ObjectMapper();

        // Kafka properties
        Properties kafkaProperties = new Properties();
        kafkaProperties.put("bootstrap.servers", bootstrapServers);
        kafkaProperties.put("group.id", groupId);

        // Create Kafka Source
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
            .setBootstrapServers(bootstrapServers)
            .setGroupId(groupId)
            .setTopics("TRN_Account_MPC4")
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .setStartingOffsets(OffsetsInitializer.latest())
            .build();

        // Account Stream
        DataStream<Account> accountStream = env
            .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Account Source")
            .map(json -> mapper.readValue(json, Account.class));

        // Process Account Stream
        DataStream<DimAccount> dimAccountStream = accountStream
            .keyBy(Account::getId)
            .process(new KeyedProcessFunction<String, Account, DimAccount>() {

                private transient ValueState<Account> lastAccountState;

                @Override
                public void open(org.apache.flink.configuration.Configuration parameters) {
                    ValueStateDescriptor<Account> descriptor = new ValueStateDescriptor<>("last-account-state", Account.class);
                    lastAccountState = getRuntimeContext().getState(descriptor);
                }

                @Override
                public void processElement(Account account, Context context, Collector<DimAccount> out) throws Exception {
                    Account lastAccount = lastAccountState.value();

                    if (lastAccount == null || !lastAccount.equals(account)) {
                        try (Connection connection = DriverManager.getConnection(
                                "jdbc:oracle:thin:@192.168.1.214:1521:dwh", "fsstraining", "fsstraining")) {

                            // Truy vấn DIM_ACCOUNT
                            String queryDimAccount = "SELECT ACCOUNT_TYPE, CCY, CR_GL, ACCOUNT_NO, MATURITY_DATE, RATE, RECORD_STAT, ACCOUNT_CLASS, EFF_DT, END_DT, UPDATE_TMS " +
                                                    "FROM FSSTRAINING.MP_DIM_ACCOUNT WHERE ACCOUNT_NO = ? AND END_DT IS NULL";
                            try (PreparedStatement dimAccountPs = connection.prepareStatement(queryDimAccount)) {
                                dimAccountPs.setString(1, account.getId());
                                ResultSet dimAccountRs = dimAccountPs.executeQuery();

                                // Ánh xạ dữ liệu từ Kafka (newDimAccount)
                                DimAccount newDimAccount = mapToDimAccount(account);
                                System.out.println("New DimAccount (from Kafka): " + newDimAccount);

                                if (dimAccountRs.next()) {
                                    // Ánh xạ dữ liệu hiện tại từ DIM_ACCOUNT
                                    DimAccount currentDimAccount = new DimAccount();
                                    currentDimAccount.setAccountType(dimAccountRs.getString("ACCOUNT_TYPE"));
                                    currentDimAccount.setCcy(dimAccountRs.getString("CCY"));
                                    currentDimAccount.setCrGl(dimAccountRs.getString("CR_GL"));
                                    currentDimAccount.setAccountNo(dimAccountRs.getString("ACCOUNT_NO"));
                                    currentDimAccount.setMaturityDate(dimAccountRs.getDate("MATURITY_DATE"));
                                    currentDimAccount.setRate(dimAccountRs.getBigDecimal("RATE"));
                                    currentDimAccount.setRecordStat(dimAccountRs.getString("RECORD_STAT"));
                                    currentDimAccount.setAccountClass(dimAccountRs.getString("ACCOUNT_CLASS"));
                                    currentDimAccount.setEffDt(dimAccountRs.getDate("EFF_DT"));
                                    currentDimAccount.setEndDt(dimAccountRs.getDate("END_DT"));
                                    currentDimAccount.setUpdateTms(dimAccountRs.getTimestamp("UPDATE_TMS"));
                                    System.out.println("Current DimAccount (from DB): " + currentDimAccount);

                                    // Kiểm tra ngày EFF_DT
                                    java.sql.Date today = new java.sql.Date(System.currentTimeMillis());
                                    boolean isEffDtToday = (currentDimAccount.getEffDt() != null &&
                                                            currentDimAccount.getEffDt().toString().equals(today.toString()));

                                    if (isEffDtToday) {
                                        // Kiểm tra sự khác biệt nếu EFF_DT là hôm nay
                                        if (hasDifferences(currentDimAccount, newDimAccount)) {
                                            // Nếu có sự khác biệt, thực hiện cập nhật bản ghi
                                            String updateCurrentQuery = "UPDATE FSSTRAINING.MP_DIM_ACCOUNT SET " +
                                                                        "ACCOUNT_TYPE = ?, CCY = ?, CR_GL = ?, MATURITY_DATE = ?, RATE = ?, RECORD_STAT = ?, " +
                                                                        "ACCOUNT_CLASS = ?, UPDATE_TMS = SYSTIMESTAMP " +
                                                                        "WHERE ACCOUNT_NO = ? AND END_DT IS NULL";
                                            try (PreparedStatement updatePs = connection.prepareStatement(updateCurrentQuery)) {
                                                updatePs.setString(1, newDimAccount.getAccountType());
                                                updatePs.setString(2, newDimAccount.getCcy());
                                                updatePs.setString(3, newDimAccount.getCrGl());
                                                updatePs.setDate(4, newDimAccount.getMaturityDate() != null
                                                        ? new java.sql.Date(newDimAccount.getMaturityDate().getTime()) : null);
                                                updatePs.setBigDecimal(5, newDimAccount.getRate() != null
                                                        ? new BigDecimal(newDimAccount.getRate().toString()) : null);
                                                updatePs.setString(6, newDimAccount.getRecordStat());
                                                updatePs.setString(7, newDimAccount.getAccountClass());
                                                updatePs.setString(8, newDimAccount.getAccountNo());
                                                updatePs.executeUpdate();
                                            }
                                        }
                                    } else {
                                        // Nếu EFF_DT không phải hôm nay, kết thúc bản ghi cũ
                                        String updateEndDateQuery = "UPDATE FSSTRAINING.MP_DIM_ACCOUNT SET END_DT = SYSDATE, UPDATE_TMS = SYSTIMESTAMP WHERE ACCOUNT_NO = ? AND END_DT IS NULL";
                                        try (PreparedStatement updatePs = connection.prepareStatement(updateEndDateQuery)) {
                                            updatePs.setString(1, newDimAccount.getAccountNo());
                                            updatePs.executeUpdate();
                                        }

                                        // Chèn bản ghi mới
                                        String insertQuery = generateInsertQueryForDimAccount(newDimAccount);
                                        try (PreparedStatement insertPs = connection.prepareStatement(insertQuery)) {
                                            insertPs.executeUpdate();
                                        }
                                    }
                                } else {
                                    // Nếu không tìm thấy bản ghi hiện tại trong DIM_ACCOUNT, thực hiện chèn mới
                                    String insertQuery = generateInsertQueryForDimAccount(newDimAccount);
                                    try (PreparedStatement insertPs = connection.prepareStatement(insertQuery)) {
                                        insertPs.executeUpdate();
                                    }
                                }
                            }
                        }
                        lastAccountState.update(account);
                    }
                }


                private boolean hasDifferences(DimAccount current, DimAccount updated) {
                    if (current == null || updated == null) {
                        return true; // Nếu một trong hai là null, coi như khác nhau
                    }

                    // In ra kết quả so sánh chi tiết trước khi thực hiện so sánh chi tiết
                    System.out.println("Comparing current and updated DimAccount:");
                    System.out.println("Current DimAccount: " + current);
                    System.out.println("Updated DimAccount: " + updated);

                    boolean result = !Objects.equals(current.getAccountType(), updated.getAccountType()) ||
                                    !Objects.equals(current.getCcy(), updated.getCcy()) ||
                                    !Objects.equals(current.getCrGl(), updated.getCrGl()) ||
                                    !Objects.equals(current.getMaturityDate(), updated.getMaturityDate()) ||
                                    !Objects.equals(current.getRate(), updated.getRate()) ||
                                    !Objects.equals(current.getRecordStat(), updated.getRecordStat()) ||
                                    !Objects.equals(current.getAccountClass(), updated.getAccountClass());

                    // In ra kết quả của so sánh
                    System.out.println("Result of comparison: " + result);
                    return result;
                }
                
            });

        // Sink DimAccount Stream to SQL
        dimAccountStream
            .map(ConsumerAccount::generateInsertQueryForDimAccount)
            .addSink(new CustomSqlSink());

        env.execute("Flink Kafka Consumer and Process");
    }

    private static DimAccount mapToDimAccount(Account account) {
        DimAccount dimAccount = new DimAccount();

        // Determine account type
        String accountType = "U";
        if (account.getPrtCode().matches("32|47|49|51|56")) {
            accountType = "Y";
        } else if (account.getPrtCode().matches("21|31|33")) {
            accountType = "S";
        } else if (account.getPrtCode().matches("35|36")) {
            accountType = "N";
        }

        dimAccount.setCrGl("1");
        dimAccount.setAccountType(accountType);
        dimAccount.setCcy(account.getCurrency());
        dimAccount.setAccountNo(account.getId());
        dimAccount.setMaturityDate(account.getDateMaturity());
        dimAccount.setRecordStat(account.getOpType().equals("D") ? "C" : "O");
        dimAccount.setAccountClass(account.getAllInOneProduct());
        dimAccount.setEffDt(new java.sql.Date(System.currentTimeMillis()));
        dimAccount.setEndDt(null);
        dimAccount.setUpdateTms(null);
        return dimAccount;
    }

    private static String generateInsertQueryForDimAccount(DimAccount dimAccount) {
        return String.format(
            "INSERT INTO FSSTRAINING.MP_DIM_ACCOUNT (account_type, ccy, cr_gl, account_no, maturity_date, rate, record_stat, account_class, eff_dt, end_dt, update_tms, act_f) " +
            "VALUES ('%s', '%s', '%s', '%s', %s, %s, '%s', '%s', %s, %s, %s, %s)",
            dimAccount.getAccountType(),
            dimAccount.getCcy(),
            dimAccount.getCrGl(),
            dimAccount.getAccountNo(),
            dimAccount.getMaturityDate() != null
                ? "TO_DATE('" + new java.sql.Date(dimAccount.getMaturityDate().getTime()) + "', 'YYYY-MM-DD')"
                : "NULL",
            dimAccount.getRate() != null ? dimAccount.getRate() : "NULL",
            dimAccount.getRecordStat(),
            dimAccount.getAccountClass(),
            dimAccount.getEffDt() != null
                ? "TO_DATE('" + new java.sql.Date(dimAccount.getEffDt().getTime()) + "', 'YYYY-MM-DD')"
                : "NULL",
            dimAccount.getEndDt() != null
                ? "TO_DATE('" + new java.sql.Date(dimAccount.getEndDt().getTime()) + "', 'YYYY-MM-DD')"
                : "NULL",
            dimAccount.getUpdateTms() != null
                ? "TO_TIMESTAMP('" + new java.sql.Timestamp(dimAccount.getUpdateTms().getTime()) + "', 'YYYY-MM-DD HH24:MI:SS.FF3')"
                : "NULL",
            dimAccount.getActF() != null ? "'" + dimAccount.getActF() + "'" : "NULL"
        );
    }
}