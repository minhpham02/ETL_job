package com.etl;

import java.lang.module.Configuration;
import java.sql.Connection;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import com.etl.Utils.KafkaSourceUtil;
import com.etl.entities.FactTransaction;
import com.etl.entities.Teller;
import com.fasterxml.jackson.databind.ObjectMapper;

import oracle.jdbc.pool.OracleDataSource;

public class KafkaTransactionConsumer {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = new StreamExecutionEnvironment();

        String bootstrapServers = "192.168.26.181:9092";
        String groupId = "flink-consumer-group";
        ObjectMapper mapper = new ObjectMapper();

        DataStream<Teller> tellerStream = env
            .fromSource(
                KafkaSourceUtil.createKafkaSource(bootstrapServers, groupId, "TRN_Teller_MPC4",
                new SimpleStringSchema()),
                WatermarkStrategy.noWatermarks(),
                "Teller Source")
            .map(json -> mapper.readValue(json, Teller.class));
        
        DataStream<FactTransaction> factTransaction = tellerStream
            .flatMap(new FactTransactionQueryFunction())
            .returns(FactTransaction.class);
        
        env.execute("ETL Fact Transaction");    
    }

    public static class FactTransactionQueryFunction<T> extends RichFlatMapFunction<T, FactTransaction> {
        private transient Connection connection;

        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception{
            super.open(parameters);
            OracleDataSource dataSource = new OracleDataSource();
            dataSource.setURL("jdbc:oracle:thin:@192.168.1.214:1521:dwh");
            dataSource.setUser("fsstraining");
            dataSource.setPassword("fsstraining");
            connection = dataSource.getConnection();         
        }

        @Override
        public void flatMap(T value, Collector<FactTransaction> out) throws Exception {

            
           
        }
    
        
    }
}
