package com.loda.jobs;

import com.loda.kafka.MyKafkaDeserializationSchema;
import com.loda.pojo.DataBean;
import com.loda.udf.Json2DataBeanV2;
import com.loda.util.FlinkUtil;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author loda
 * @Date 2023/4/28 17:24
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
//人气统计
//规则：观众需要在直播间1分钟以上，人气+1；如果半个小时内，重复进入，只算一次人气
//需要使用定时器
public class Hello04LiveAudiencePopularity {
    public static void main(String[] args) throws Exception {
        FlinkUtil.env.setParallelism(1);
        //kafka source
        DataStream<Tuple2<String, String>> kafkaStream = FlinkUtil.createKafkaStreamV2(args[0], MyKafkaDeserializationSchema.class);

        SingleOutputStreamOperator<DataBean> dataBeanStream = kafkaStream.process(new Json2DataBeanV2());

        SingleOutputStreamOperator<DataBean> liveStream = dataBeanStream.filter(bean -> bean.getEventId().startsWith("live"));

        SingleOutputStreamOperator<DataBean> enterOrLeaveStream = liveStream.filter(bean -> "liveEnter".equals(bean.getEventId()) || "liveLeave".equals(bean.getEventId()));

        //按照设备ID和直播间ID进行KeyBy
        KeyedStream<DataBean, Tuple2<String, String>> anchorIdAndDeviceIdStream = enterOrLeaveStream.keyBy(new KeySelector<DataBean, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> getKey(DataBean bean) throws Exception {
                String deviceId = bean.getDeviceId();
                String anchor_id = bean.getProperties().get("anchor_id").toString();
                return Tuple2.of(anchor_id, deviceId);
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> popularity = anchorIdAndDeviceIdStream.process(new KeyedProcessFunction<Tuple2<String, String>, DataBean, Tuple2<String, Integer>>() {
            //定义keyed状态
            ValueState<Long> inState;
            ValueState<Long> outState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<Long> inValueStateDescriptor = new ValueStateDescriptor<>("in-time", Types.LONG);
                inState = getRuntimeContext().getState(inValueStateDescriptor);

                ValueStateDescriptor<Long> outValueStateDescriptor = new ValueStateDescriptor<>("out-time", Types.LONG);
                outState = getRuntimeContext().getState(outValueStateDescriptor);
            }

            @Override
            public void processElement(DataBean bean, KeyedProcessFunction<Tuple2<String, String>,
                    DataBean, Tuple2<String, Integer>>.Context ctx, Collector<Tuple2<String, Integer>> out)
                    throws Exception {
                Long timestamp = bean.getTimestamp();
                //进入时设置定时器
                if ("liveEnter".equals(bean.getEventId())) {
                    inState.update(timestamp);
                    //当观众不退出，一直在直播间，定时器到点会触发
                    ctx.timerService().registerProcessingTimeTimer(timestamp + 60000 + 1);
                } else {
                    //退出时，判断
                    //如果观众退出时间与进入时间小于1分钟，则删除进入时创建的定时器
                    outState.update(timestamp);
                    Long inTime = inState.value();
                    if ((timestamp - inTime) < 60000) {
                        ctx.timerService().deleteEventTimeTimer(inTime + 60000 + 1);
                    }
                }
            }

            //触发定时器时需要做的操作
            @Override
            public void onTimer(long timestamp, KeyedProcessFunction<Tuple2<String, String>, DataBean, Tuple2<String, Integer>>.OnTimerContext ctx,
                                Collector<Tuple2<String, Integer>> out) throws Exception {
                Long outTime = outState.value();
                //如果退出时间为空，最新一次登录或者还没退出
                if (outTime == null) {
                    out.collect(Tuple2.of(ctx.getCurrentKey().f0, 1));
                } else {
                    Long inTime = inState.value();
                    //本次登录时间减上次退出时间，判断逻辑上不完整，还需想想怎么完善 TODO
                    if (inTime - outTime > 30 * 60000) {
                        out.collect(Tuple2.of(ctx.getCurrentKey().f0, 1));
                    }
                }
            }
        });
        
        popularity.keyBy(value -> value.f0).sum(1).print("+++++++++++++++++");

        //env exec
        FlinkUtil.env.execute();
    }
}
