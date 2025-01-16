package com.etl;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Objects;
import java.util.Properties;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import com.etl.Utils.CustomSqlSink;
import com.etl.entities.DimProduct;
import com.etl.entities.Product;

public class ConsumerProduct {
    public static void main(String[] args) throws Exception {
        // Set up Flink environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///tmp/flink-checkpoints");
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        String bootstrapServers = "192.168.26.181:9092";
        String groupId = "flink-consumer-group_03";
        ObjectMapper mapper = new ObjectMapper();

        // Kafka properties
        Properties kafkaProperties = new Properties();
        kafkaProperties.put("bootstrap.servers", bootstrapServers);
        kafkaProperties.put("group.id", groupId);

        // Create Kafka Source
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
        .setBootstrapServers(bootstrapServers)
        .setGroupId(groupId)
        .setTopics("TRN_product_02") // Topic name
        .setValueOnlyDeserializer(new SimpleStringSchema())
        .setStartingOffsets(OffsetsInitializer.committedOffsets()) // Use committed offsets
        .setProperty("enable.auto.commit", "false") // Disable Kafka auto-commit
        .build();

        // Product Stream
        DataStream<Product> productStream = env
        .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Product Source")
        .map(json -> {
            try {
                return mapper.readValue(json, Product.class);  // Ánh xạ JSON thành đối tượng Account
            } catch (Exception e) {
                System.err.println("Error parsing JSON: " + e.getMessage());
                System.err.println("Invalid JSON: " + json);
                return null; // Bỏ qua bản ghi không hợp lệ
            }
        }).filter(Objects::nonNull); // Loại bỏ các bản ghi null

        // Process Product Stream
        DataStream<DimProduct> dimProductStream = productStream
            .keyBy(Product::getProductNo) // Key by productNo
            .process(new KeyedProcessFunction<Long, Product, DimProduct>() {

            private transient ValueState<Product> lastProductState;

            @Override
            public void open(Configuration parameters) {
                // Define the state descriptor for the last product
                ValueStateDescriptor<Product> descriptor = new ValueStateDescriptor<>("last-product-state", Product.class);
                lastProductState = getRuntimeContext().getState(descriptor);
            }

            @Override
            public void processElement(Product product, Context context, Collector<DimProduct> out) throws Exception {
                DimProduct newDimProduct = mapToDimProduct(product); // Chuyển Product sang DimProduct
                try (Connection connection = DriverManager.getConnection(
                        "jdbc:oracle:thin:@192.168.1.214:1521:dwh", "fsstraining", "fsstraining")) {

                    // Query để kiểm tra bản ghi trong DIM_PRODUCT
                    String queryDimProduct = "SELECT PRODUCT_CATEGORY, PRODUCT_CODE, PRODUCT_TYPE, RECORD_STAT, EFF_DT " +
                                                "FROM FSSTRAINING.DIM_PRODUCT " +
                                                "WHERE PRODUCT_NO = ? AND END_DT IS NULL";

                    try (PreparedStatement dimProductPs = connection.prepareStatement(queryDimProduct)) {
                        dimProductPs.setLong(1, product.getProductNo());
                        ResultSet dimProductRs = dimProductPs.executeQuery();

                    if (dimProductRs.next()) {
                        // Bản ghi tồn tại, lấy dữ liệu từ DB
                        String dbProductCategory = dimProductRs.getString("PRODUCT_CATEGORY");
                        String dbProductCode = dimProductRs.getString("PRODUCT_CODE");
                        String dbProductType = dimProductRs.getString("PRODUCT_TYPE");
                        String dbRecordStat = dimProductRs.getString("RECORD_STAT");
                        java.sql.Date dbEffDt = dimProductRs.getDate("EFF_DT");

                        // So sánh eff_dt (ngày tháng năm)
                        java.sql.Date currentDate = new java.sql.Date(System.currentTimeMillis());
                        if (!dbEffDt.toLocalDate().isEqual(currentDate.toLocalDate())) {
                            // Nếu eff_dt không trùng ngày hiện tại:
                            // 1. Cập nhật bản ghi cũ
                            String updateOldRecord = "UPDATE FSSTRAINING.DIM_PRODUCT " +
                                                        "SET END_DT = SYSDATE, UPDATE_TMS = SYSTIMESTAMP " +
                                                        "WHERE PRODUCT_NO = ? AND END_DT IS NULL";
                            try (PreparedStatement updatePs = connection.prepareStatement(updateOldRecord)) {
                                updatePs.setLong(1, product.getProductNo());
                                updatePs.executeUpdate();
                                System.out.println("Da cap nhat END_DT cua ban ghi cu thanh cong");
                            }
                            // 2. Thêm bản ghi mới
                            out.collect(newDimProduct);
                            System.out.println("Da chen moi thanh cong");
                        } else {
                            // Nếu eff_dt trùng ngày hiện tại:
                            // So sánh các trường khác
                            if (!dbProductCategory.equals(newDimProduct.getProductCategory()) ||
                                !dbProductCode.equals(newDimProduct.getProductCode()) ||
                                !dbProductType.equals(newDimProduct.getProductType()) ||
                                !dbRecordStat.equals(newDimProduct.getRecordStat())) {

                                // Nếu bất kỳ trường nào khác: cập nhật bản ghi
                                String updateFields = 
                                "UPDATE FSSTRAINING.DIM_PRODUCT SET " +
                                "PRODUCT_CATEGORY = COALESCE(?, PRODUCT_CATEGORY), " +
                                "PRODUCT_CODE = COALESCE(?, PRODUCT_CODE), " +
                                "PRODUCT_TYPE = COALESCE(?, PRODUCT_TYPE), " +
                                "RECORD_STAT = COALESCE(?, RECORD_STAT), " +
                                "UPDATE_TMS = SYSTIMESTAMP " +
                                "WHERE PRODUCT_NO = ? AND END_DT IS NULL";
                                try (PreparedStatement updatePs = connection.prepareStatement(updateFields)) {
                                    updatePs.setString(1, newDimProduct.getProductCategory() != null 
                                    ? newDimProduct.getProductCategory() 
                                    : null);
                                    updatePs.setString(2, newDimProduct.getProductCode() != null 
                                    ? newDimProduct.getProductCode() 
                                    : null); // CCY
                                    updatePs.setString(3, newDimProduct.getProductType() != null 
                                    ? newDimProduct.getProductType() 
                                    : null);
                                    updatePs.setString(4, newDimProduct.getRecordStat() != null 
                                    ? newDimProduct.getRecordStat() 
                                    : null);
                                    updatePs.setLong(5, product.getProductNo());
                                    updatePs.executeUpdate();
                                    System.out.println("Da cap nhat ban ghi thanh cong");
                                }
                            } else {
                                // Nếu không khác gì, pass
                                System.out.println("Ban ghi khong co thay doi, khong can hanh dong");
                            }
                        }
                    } else {
                        // Nếu không có bản ghi nào: thêm mới
                        out.collect(newDimProduct);
                        System.out.println("Da chen moi thanh cong");
                    }}
                } catch (Exception e) {
                    System.err.println("Loi xay ra khi xu ly: " + e.getMessage());
                    e.printStackTrace();
                }
                // Cập nhật state với bản ghi mới nhất
                lastProductState.update(product);
            }
        });

        // Sink DimProduct Stream to SQL
        dimProductStream
        .map(dimProduct -> {
            return String.format(
                "INSERT INTO FSSTRAINING.DIM_PRODUCT " +
                "(PD_DIM_ID, PRODUCT_NO, PRODUCT_CATEGORY, PRODUCT_CODE, PRODUCT_TYPE, RECORD_STAT, EFF_DT, END_DT, UPDATE_TMS) " +
                "VALUES (FSSTRAINING.PD_DIM_ID_SEQ.NEXTVAL, %d, '%s', '%s', '%s', '%s', SYSDATE, NULL, NULL)",
                dimProduct.getProductNo(),
                dimProduct.getProductCategory(),
                dimProduct.getProductCode(),
                dimProduct.getProductType(),
                dimProduct.getRecordStat()
            );
        })
        .addSink(new CustomSqlSink()); // Sink to SQL
        env.execute("Flink Kafka Consumer and Process");
    }

    // Map Product to DimProduct
    private static DimProduct mapToDimProduct(Product product) {
        DimProduct dimProduct = new DimProduct();
        dimProduct.setProductNo(product.getProductNo());
        dimProduct.setProductCategory(mapProductCategory(product.getLocTerm()));
        dimProduct.setProductCode(product.getCategory());
        dimProduct.setProductType(product.getSubProduct());
        if (product.getOpType() == null) {
            dimProduct.setRecordStat(null); } 
        else {
            dimProduct.setRecordStat("D".equals(product.getOpType()) ? "D" : "A"); }        
        dimProduct.setEffDt(new java.sql.Date(System.currentTimeMillis())); // Current date for EFF_DT
        dimProduct.setEndDt(null); // END_DT set to null initially
        dimProduct.setUpdateTms(null); // UPDATE_TMS set to null initially
        return dimProduct;
    }

    // Map locTerm to product category
    private static String mapProductCategory(String locTerm) {
        if (locTerm != null) {
            if (locTerm.endsWith("D")) {
                return locTerm;
            } else {try {
                int locTermValue = Integer.parseInt(locTerm);
                if (locTermValue <= 12) {
                    return "SHORTTERM";} 
                else if (locTermValue <= 60) {
                    return "MEDIUMTERM";} 
                else {
                    return "LONGTERM";}} 
            catch (NumberFormatException e) {
                return "UNKNOWN";}
            }
        }
        return null;
    }
}