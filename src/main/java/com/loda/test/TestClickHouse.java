package com.loda.test;

import com.loda.kafka.MyKafkaDeserializationSchema;
import com.loda.util.FlinkUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * @Author loda
 * @Date 2023/4/26 17:43
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class TestClickHouse {
    public static void main(String[] args) throws Exception {
        DataStream<Tuple2<String, String>> kafkaStream = FlinkUtil.createKafkaStreamV2(args[0], MyKafkaDeserializationSchema.class);

        kafkaStream.print();



        FlinkUtil.env.execute();
    }
}
