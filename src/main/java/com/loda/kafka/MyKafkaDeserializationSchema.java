package com.loda.kafka;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.charset.StandardCharsets;

/**
 * @Author loda
 * @Date 2023/4/26 17:14
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class MyKafkaDeserializationSchema implements KafkaDeserializationSchema<Tuple2<String, String>> {
    @Override
    public boolean isEndOfStream(Tuple2<String, String> nextElement) {
        return false;
    }

    @Override
    public Tuple2<String, String> deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
        String topic = record.topic();
        int partition = record.partition();
        long offset = record.offset();
        String id = topic + "-" + partition + "-" + offset;
//        byte[] value1 = record.value();
        String value = new String(record.value(), StandardCharsets.UTF_8);
        return Tuple2.of(id, value);
    }

    @Override
    public TypeInformation<Tuple2<String, String>> getProducedType() {
        return Types.TUPLE(Types.STRING, Types.STRING);
    }
}
