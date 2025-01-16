package com.etl;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import com.etl.entities.FactTransaction;
import com.etl.entities.Teller;
import com.fasterxml.jackson.databind.ObjectMapper;

import oracle.jdbc.pool.OracleDataSource;

public class KafkaTransactionConsumer {
    public static void main(String[] args) throws Exception {
        String bootstrapServers = "192.168.26.181:9092";
        String groupId = "flink-consumer-group";
        ObjectMapper mapper = new ObjectMapper();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setGroupId(groupId)
                .setTopics("TRN_Teller_MPC4")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<Teller> tellerStream = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Teller Source")
                .map(new MapFunction<String, Teller>() {
                    @Override
                    public Teller map(String json) throws Exception {
                        return mapper.readValue(json, Teller.class);
                    }
                });

        @SuppressWarnings({ "rawtypes", "unchecked" })
        DataStream<FactTransaction> factTransaction = tellerStream
                .flatMap(new FactTransactionQueryFunction())
                .returns(FactTransaction.class);

        env.execute("MP_FactTransaction");
    }

    public static class FactTransactionQueryFunction<T> extends RichFlatMapFunction<T, FactTransaction> {
        private transient Connection connection;

        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
            super.open(parameters);
            OracleDataSource dataSource = new OracleDataSource();
            dataSource.setURL("jdbc:oracle:thin:@192.168.1.214:1521:dwh");
            dataSource.setUser("fsstraining");
            dataSource.setPassword("fsstraining");
            connection = dataSource.getConnection();
        }

        @Override
        public void flatMap(T value, Collector<FactTransaction> out) throws Exception {
            if (value instanceof Teller) {
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

                if (OpType != null) {
                    if (!"D".equals(OpType)) {
                        RecordStat = "O";
                    } else if ("D".equals(OpType)) {
                        RecordStat = "C";
                    } else {
                        RecordStat = "";
                    }
                }

                AuthStat = (Authoriser != null) ? "A" : "U";
                String account1 = teller.getAccount1() != null ? teller.getAccount1() : "";
                String account2 = teller.getAccount2() != null ? teller.getAccount2() : "";

                String mergeSql = "MERGE INTO FSSTRAINING.MP_FACT_TRANSACTION target "
                        + "USING (SELECT ? AS TXN_CCY, ? AS PRODUCT_CODE, ? AS TRN_REF_NO, ? AS LCY_AMOUNT, ? AS TXN_AMOUNT, ? AS CST_DIM_ID, ? AS RECORD_STAT, ? AS AUTH_STAT, ? AS TXN_ACC, ? AS OFS_ACC, ? AS TRN_DT FROM DUAL) source "
                        + "ON (target.TRN_REF_NO = source.TRN_REF_NO) "
                        + "WHEN MATCHED THEN "
                        + "UPDATE SET TXN_CCY = CASE WHEN target.TXN_CCY != source.TXN_CCY THEN source.TXN_CCY ELSE target.TXN_CCY END, "
                        + "PRODUCT_CODE = CASE WHEN target.PRODUCT_CODE != source.PRODUCT_CODE THEN source.PRODUCT_CODE ELSE target.PRODUCT_CODE END, "
                        + "LCY_AMOUNT = CASE WHEN (target.LCY_AMOUNT != source.LCY_AMOUNT AND source.LCY_AMOUNT !=0) THEN source.LCY_AMOUNT ELSE target.LCY_AMOUNT END, "
                        + "TXN_AMOUNT = CASE WHEN (target.TXN_AMOUNT != source.TXN_AMOUNT AND source.TXN_AMOUNT != 0) THEN source.TXN_AMOUNT ELSE target.TXN_AMOUNT END, "
                        + "CST_DIM_ID = CASE WHEN target.CST_DIM_ID != source.CST_DIM_ID THEN source.CST_DIM_ID ELSE target.CST_DIM_ID END, "
                        + "RECORD_STAT = CASE WHEN target.RECORD_STAT != source.RECORD_STAT THEN source.RECORD_STAT ELSE target.RECORD_STAT END, "
                        + "AUTH_STAT = CASE WHEN target.AUTH_STAT != source.AUTH_STAT THEN source.AUTH_STAT ELSE target.AUTH_STAT END, "
                        + "TXN_ACC = CASE WHEN target.TXN_ACC != source.TXN_ACC THEN source.TXN_ACC ELSE target.TXN_ACC END, "
                        + "OFS_ACC = CASE WHEN target.OFS_ACC != source.OFS_ACC THEN source.OFS_ACC ELSE target.OFS_ACC END, "
                        + "TRN_DT = CASE WHEN target.TRN_DT != source.TRN_DT THEN source.TRN_DT ELSE target.TRN_DT END, "
                        + "ETL_DATE = CURRENT_DATE "
                        + "WHEN NOT MATCHED THEN "
                        + "INSERT (TXN_CCY, PRODUCT_CODE, TRN_REF_NO, LCY_AMOUNT, TXN_AMOUNT, CST_DIM_ID, RECORD_STAT, AUTH_STAT, TXN_ACC, OFS_ACC, TRN_DT, ETL_DATE) "
                        + "VALUES (source.TXN_CCY, source.PRODUCT_CODE, source.TRN_REF_NO, source.LCY_AMOUNT, source.TXN_AMOUNT, source.CST_DIM_ID, source.RECORD_STAT, source.AUTH_STAT, source.TXN_ACC, source.OFS_ACC, source.TRN_DT, CURRENT_DATE)";

                PreparedStatement statement = connection.prepareStatement(mergeSql);
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
