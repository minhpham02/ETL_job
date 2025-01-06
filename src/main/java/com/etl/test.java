package com.etl;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;

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

public class test {

    public static void main(String[] args) throws Exception {
        // Set up Flink environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String bootstrapServers = "192.168.26.181:9092";
        String groupId = "flink-consumer-group";
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

                            // Check if ACCOUNT_NO already exists in MP_DIM_ACCOUNT
                            String queryDimAccount = "SELECT ACCOUNT_NO FROM FSSTRAINING.MP_DIM_ACCOUNT WHERE ACCOUNT_NO = ?";
                            try (PreparedStatement dimAccountPs = connection.prepareStatement(queryDimAccount)) {
                                dimAccountPs.setString(1, account.getId());
                                ResultSet dimAccountRs = dimAccountPs.executeQuery();

                                // If account exists in MP_DIM_ACCOUNT, update END_DT
                                if (dimAccountRs.next()) {
                                    String updateEndDateQuery = "UPDATE FSSTRAINING.MP_DIM_ACCOUNT SET END_DT = SYSDATE WHERE ACCOUNT_NO = ?";
                                    try (PreparedStatement updatePs = connection.prepareStatement(updateEndDateQuery)) {
                                        updatePs.setString(1, account.getId());
                                        updatePs.executeUpdate();
                                    }
                                }
                            }

                            // Continue processing as before, query TRN_AZ_ACCOUNT or TRN_ACCR_ACCT_CR
                            String queryAzAccount = "SELECT INTEREST_RATE FROM TRN_AZ_ACCOUNT WHERE ID = ?";
                            try (PreparedStatement azAccountPs = connection.prepareStatement(queryAzAccount)) {
                                azAccountPs.setString(1, account.getId());
                                ResultSet azAccountRs = azAccountPs.executeQuery();

                                if (azAccountRs.next()) {
                                    DimAccount dimAccount = mapToDimAccount(account);
                                    dimAccount.setRate(azAccountRs.getBigDecimal("INTEREST_RATE"));
                                    out.collect(dimAccount);
                                    lastAccountState.update(account);
                                    return;
                                }
                            }

                            // Query TRN_ACCR_ACCT_CR
                            String queryAccrAcctCr = "SELECT CR_INT_RATE FROM FSSTRAINING.TRN_ACCR_ACCT_CR WHERE ACCOUNT_NUMBER = ?";
                            try (PreparedStatement accrAcctCrPs = connection.prepareStatement(queryAccrAcctCr)) {
                                accrAcctCrPs.setString(1, account.getId());
                                ResultSet accrAcctCrRs = accrAcctCrPs.executeQuery();

                                if (accrAcctCrRs.next()) {
                                    DimAccount dimAccount = mapToDimAccount(account);

                                    String crIntRate = accrAcctCrRs.getString("CR_INT_RATE");
                                    BigDecimal rate = null;

                                    // Process CR_INT_RATE
                                    if (crIntRate != null) {
                                        if (crIntRate.contains("#")) {
                                            String trimmedRate = crIntRate.substring(crIntRate.lastIndexOf("#") + 1).trim();
                                            rate = new BigDecimal(trimmedRate);
                                        } else {
                                            rate = new BigDecimal(crIntRate);
                                        }
                                    }

                                    dimAccount.setRate(rate);
                                    out.collect(dimAccount);
                                    lastAccountState.update(account);
                                    return;
                                }
                            }

                            // If no rate found in either table
                            DimAccount dimAccount = mapToDimAccount(account);
                            dimAccount.setRate(null);
                            out.collect(dimAccount);
                            lastAccountState.update(account);
                        }
                    }
                }

            });

        // Sink DimAccount Stream to SQL
        dimAccountStream
            .map(test::generateInsertQueryForDimAccount)
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