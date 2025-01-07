package com.etl;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
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
            .setTopics("TRN_product_01")
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .setStartingOffsets(OffsetsInitializer.latest()) // Start from the latest offset
            .build();

        // Product Stream
        DataStream<Product> productStream = env
            .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Product Source")
            .map(json -> mapper.readValue(json, Product.class)); // Deserialize to Product

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
                    // Convert Product to DimProduct
                    DimProduct newDimProduct = mapToDimProduct(product);

                    boolean isUpdated = false; // Flag to track if update occurred

                    // Check for duplicates and process update/insert
                    try (Connection connection = DriverManager.getConnection(
                            "jdbc:oracle:thin:@192.168.1.214:1521:dwh", "fsstraining", "fsstraining")) {

                        // Check if PRODUCT_NO exists in DIM_PRODUCT
                        String queryDimProduct = "SELECT PRODUCT_CATEGORY, PRODUCT_CODE, PRODUCT_TYPE, RECORD_STAT FROM FSSTRAINING.DIM_PRODUCT WHERE PRODUCT_NO = ? AND END_DT IS NULL";
                        try (PreparedStatement dimProductPs = connection.prepareStatement(queryDimProduct)) {
                            dimProductPs.setLong(1, product.getProductNo());
                            ResultSet dimProductRs = dimProductPs.executeQuery();

                            if (dimProductRs.next()) {
                                // If there is a matching record with PRODUCT_NO
                                String dbProductCategory = dimProductRs.getString("PRODUCT_CATEGORY");
                                String dbProductCode = dimProductRs.getString("PRODUCT_CODE");
                                String dbProductType = dimProductRs.getString("PRODUCT_TYPE");
                                String dbRecordStat = dimProductRs.getString("RECORD_STAT");

                                // Check if all fields match
                                if (dbProductCategory.equals(newDimProduct.getProductCategory()) &&
                                    dbProductCode.equals(newDimProduct.getProductCode()) &&
                                    dbProductType.equals(newDimProduct.getProductType()) &&
                                    dbRecordStat.equals(newDimProduct.getRecordStat())) {
                                    // If identical, do nothing
                                    System.out.println("Record already exists, no update or insert needed.");
                                } else {
                                    // If there is a difference in any field
                                    // Update the old record
                                    String updateEndDateQuery = "UPDATE FSSTRAINING.DIM_PRODUCT SET END_DT = SYSDATE, UPDATE_TMS = SYSTIMESTAMP WHERE PRODUCT_NO = ?";
                                    try (PreparedStatement updatePs = connection.prepareStatement(updateEndDateQuery)) {
                                        updatePs.setLong(1, product.getProductNo());
                                        updatePs.executeUpdate();
                                        System.out.println("Successfully updated old record.");
                                        isUpdated = true; // Mark as updated
                                    }

                                    // Insert a new record if updated
                                    if (isUpdated) {
                                        out.collect(newDimProduct); // Collect the new DimProduct to output
                                    }
                                }
                            } else {
                                // If no matching record, insert a new one
                                out.collect(newDimProduct); // Collect the new DimProduct to output
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    lastProductState.update(product); // Update state with the latest product
                }
            });

        // Sink DimProduct Stream to SQL
        dimProductStream
            .map(dimProduct -> {
                // Chuyển đổi đối tượng DimProduct thành câu lệnh SQL dạng chuỗi
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
        dimProduct.setRecordStat(product.getOpType().equals("D") ? "D" : "A");
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
            } else {
                try {
                    int locTermValue = Integer.parseInt(locTerm);
                    if (locTermValue <= 12) {
                        return "SHORTTERM";
                    } else if (locTermValue <= 60) {
                        return "MEDIUMTERM";
                    } else {
                        return "LONGTERM";
                    }
                } catch (NumberFormatException e) {
                    return "UNKNOWN";
                }
            }
        }
        return null;
    }
}