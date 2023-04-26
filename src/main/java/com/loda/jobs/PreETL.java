package com.loda.jobs;

import com.loda.util.FlinkUtil;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.io.IOException;

/**
 * @Author loda
 * @Date 2023/4/25 18:52
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class PreETL {
    public static void main(String[] args) throws Exception {
        //source
        DataStream<String> kafkaStream = FlinkUtil.createKafkaStream(args[0], SimpleStringSchema.class);

        //transform

        //sink
        kafkaStream.print();

        FlinkUtil.env.execute();
    }
}
