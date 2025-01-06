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

import com.etl.Util.KafkaSourceUtil;
import com.etl.entities.DimProduct;
import com.etl.entities.Product;

import oracle.jdbc.pool.OracleDataSource;

public class KafkaProductConsumer {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String bootstrapServers = "192.168.26.181:9092";
        String groupId = "flink-consumer-group";
        ObjectMapper mapper = new ObjectMapper();

        DataStream<Product> productStream = env
                .fromSource(
                        KafkaSourceUtil.createKafkaSource(bootstrapServers, groupId, "TRN_Product_MPC4",
                                new SimpleStringSchema()),
                        WatermarkStrategy.noWatermarks(),
                        "Product Source")
                .map(json -> mapper.readValue(json, Product.class));

        DataStream<DimProduct> dimProduct = productStream
            .flatMap(new DimProductQueryFunction())
            .returns(DimProduct.class); 
    }

    public static class DimProductQueryFunction<T> extends RichFlatMapFunction<T, DimProduct> {
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
        public void flatMap(T value, Collector<DimProduct> out) throws Exception {
            if (value instanceof Product) {
                Product product = (Product) value;
        
                String selectQuery = "SELECT PRODUCT_NO FROM FSSTRAINING.MP_DIM_PRODUCT WHERE PRODUCT_NO = ? AND END_DT IS NULL";
                Long existingProductNo = null;
        
                try (PreparedStatement preparedStatement = connection.prepareStatement(selectQuery)) {
                    preparedStatement.setLong(1, product.getProductNo().longValue());
                    try (ResultSet resultSet = preparedStatement.executeQuery()) {
                        if (resultSet.next()) {
                            existingProductNo = resultSet.getLong("PRODUCT_NO");
                        }
                    }
                }
        
                if (existingProductNo != null) {
                    String updateQuery = "UPDATE FSSTRAINING.DIM_PRODUCT SET END_DT = CURRENT_DATE WHERE PRODUCT_NO = ?";
                    try (PreparedStatement preparedStatement = connection.prepareStatement(updateQuery)) {
                        preparedStatement.setLong(1, existingProductNo);
                        preparedStatement.executeUpdate();
                        System.out.println("Updated END_DT for PRODUCT_NO: " + existingProductNo);
                    }
                }
        
                String insertQuery = "INSERT INTO FSSTRAINING.MP_DIM_PRODUCT (PRODUCT_NO, PRODUCT_CATEGORY, PRODUCT_CODE, PRODUCT_TYPE, RECORD_STAT, EFF_DT, END_DT, UPDATE_TMS) "
                        + "VALUES (?, ?, ?, ?, ?, ?, NULL, CURRENT_TIMESTAMP)";
                try (PreparedStatement preparedStatement = connection.prepareStatement(insertQuery)) {
                    preparedStatement.setLong(1, product.getProductNo().longValue());
                    preparedStatement.setString(2, product.getLocTerm());
                    preparedStatement.setString(3, product.getCategory());
                    preparedStatement.setString(4, product.getSubProduct());
                    preparedStatement.setString(5, product.getOpType());
                    preparedStatement.setDate(6, new java.sql.Date(product.getEffectiveDate().getTime()));
        
                    preparedStatement.executeUpdate();
                    System.out.println("Inserted new productNo: " + product.getProductNo());
                }
        
                DimProduct dimProduct = new DimProduct();
                dimProduct.setProductNo(product.getProductNo().longValue());
                dimProduct.setProductCategory(product.getLocTerm());
                dimProduct.setProductCode(product.getCategory());
                dimProduct.setProductType(product.getSubProduct());
                dimProduct.setRecordStat(product.getOpType());
                dimProduct.setEffDt(new java.sql.Date(product.getEffectiveDate().getTime()));
                dimProduct.setEndDt(null);
                dimProduct.setUpdateTms(new java.sql.Timestamp(System.currentTimeMillis()));
        
                out.collect(dimProduct);
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