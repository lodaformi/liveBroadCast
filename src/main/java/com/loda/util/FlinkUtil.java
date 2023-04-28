package com.loda.util;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * @Author loda
 * @Date 2023/4/24 20:51
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class FlinkUtil {
    public static final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    public static ParameterTool parameterTool = null;

    public static <T> DataStream<T> createKafkaStream(String args, Class<? extends DeserializationSchema<T>> deserialization) throws InstantiationException, IllegalAccessException, IOException {
        parameterTool = ParameterTool.fromPropertiesFile(args);

        long ckptInterval = parameterTool.getLong("checkpoint.interval", 30000L);
        String ckptPath = parameterTool.get("checkpoint.path");

        env.enableCheckpointing(ckptInterval, CheckpointingMode.EXACTLY_ONCE);
        env.setStateBackend(new FsStateBackend(ckptPath));
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        List<String> topics = Arrays.asList(parameterTool.get("kafka.input.topics").split(","));

        Properties properties = parameterTool.getProperties();

        System.out.println(properties.toString());

        FlinkKafkaConsumer<T> kafkaConsumer = new FlinkKafkaConsumer<>(topics, deserialization.newInstance(), properties);
        kafkaConsumer.setCommitOffsetsOnCheckpoints(false);

        return env.addSource(kafkaConsumer);
    }

    public static <T> DataStream<T> createKafkaStreamV2(String args, Class<? extends KafkaDeserializationSchema<T>> deserialization) throws InstantiationException, IllegalAccessException, IOException {
        parameterTool = ParameterTool.fromPropertiesFile(args);

        long ckptInterval = parameterTool.getLong("checkpoint.interval", 30000L);
        String ckptPath = parameterTool.get("checkpoint.path");

        env.enableCheckpointing(ckptInterval, CheckpointingMode.EXACTLY_ONCE);
//        env.setStateBackend(new FsStateBackend(ckptPath));
        env.setStateBackend(new RocksDBStateBackend(ckptPath, true));
//        env.setStateBackend(new embed)
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        List<String> topics = Arrays.asList(parameterTool.get("kafka.input.topics").split(","));

        Properties properties = parameterTool.getProperties();

//        System.out.println(properties.toString());

        FlinkKafkaConsumer<T> kafkaConsumer = new FlinkKafkaConsumer<>(topics, deserialization.newInstance(), properties);
        kafkaConsumer.setCommitOffsetsOnCheckpoints(false);

        return env.addSource(kafkaConsumer);
    }


}
