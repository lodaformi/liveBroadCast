package com.loda.jobs;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.loda.kafka.MyKafkaDeserializationSchema;
import com.loda.pojo.DataBean;
import com.loda.udf.Hello04LiveAudienceProcessFunction;
import com.loda.udf.Json2DataBeanV2;
import com.loda.util.FlinkUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.nio.charset.StandardCharsets;

/**
 * @Author loda
 * @Date 2023/4/28 14:47
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 * 统计主播或者直播间累计访问人数uv，累计pv，实时在线人数
 */
public class Hello04LiveAudienceCount {
    public static void main(String[] args) throws Exception {
        FlinkUtil.env.setParallelism(1);
        //kafka source
        DataStream<Tuple2<String, String>> kafkaStream = FlinkUtil.createKafkaStreamV2(args[0], MyKafkaDeserializationSchema.class);

        SingleOutputStreamOperator<DataBean> dataBeanStream = kafkaStream.process(new Json2DataBeanV2());
//        dataBeanStream.print();

        SingleOutputStreamOperator<DataBean> liveStream = dataBeanStream.filter(bean -> bean.getEventId().startsWith("live"));
//        liveStream.print();

        SingleOutputStreamOperator<DataBean> enterOrLeaveStream = liveStream.filter(bean -> "liveEnter".equals(bean.getEventId()) || "liveLeave".equals(bean.getEventId()));
//        enterOrLeaveStream.print();

        KeyedStream<DataBean, String> anchorStream = enterOrLeaveStream.keyBy(bean -> bean.getProperties().get("anchor_id").toString());
//        anchorStream.print();
        anchorStream.process(new Hello04LiveAudienceProcessFunction()).print();

        //env exec
        FlinkUtil.env.execute();
    }
}
