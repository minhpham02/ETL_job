package com.etl.Utils;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.logging.Logger;

public class CustomSqlSink extends RichSinkFunction<String> {

    private static final Logger logger = Logger.getLogger(CustomSqlSink.class.getName());

    private Connection connection;
    private int insertCount;

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
        super.open(parameters);
        // Kết nối đến cơ sở dữ liệu
        try {
            connection = DriverManager.getConnection(
                "jdbc:oracle:thin:@192.168.1.214:1521:dwh",
                "fsstraining",
                "fsstraining"
            );
            logger.info("Connected to the database successfully.");
        } catch (SQLException e) {
            logger.severe("Failed to connect to the database: " + e.getMessage());
            throw new RuntimeException("Database connection failed", e);
        }
        insertCount = 0;
    }

    @Override
    public void invoke(String sql, Context context) throws Exception {
        // Sử dụng PreparedStatement để thực thi SQL
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.executeUpdate();
            insertCount++;
            logger.info("Executed SQL successfully. Total Inserts: " + insertCount);
        } catch (SQLException e) {
            logger.severe("Failed to execute SQL: " + sql + ". Error: " + e.getMessage());
            throw e;
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        // Đóng kết nối cơ sở dữ liệu
        if (connection != null && !connection.isClosed()) {
            try {
                connection.close();
                logger.info("Database connection closed.");
            } catch (SQLException e) {
                logger.warning("Failed to close the database connection: " + e.getMessage());
            }
        }
    }
}
