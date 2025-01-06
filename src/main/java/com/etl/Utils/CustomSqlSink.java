package com.etl.Utils;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import com.etl.entities.DimAccount;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class CustomSqlSink implements SinkFunction<DimAccount> {

    // Đổi phạm vi của phương thức insert thành public
    public static void insert(String sql) {
        Connection connection = null;
        Statement statement = null;
        try {
            connection = DriverManager.getConnection(
                    "jdbc:oracle:thin:@192.168.1.214:1521:dwh",
                    "fsstraining",
                    "fsstraining"
            );
            statement = connection.createStatement();
            statement.execute(sql);
            System.out.println("SQL executed successfully: " + sql);
        } catch (Exception e) {
            System.err.println("Failed to execute SQL: " + sql);
            e.printStackTrace();
        } 
    }
}
