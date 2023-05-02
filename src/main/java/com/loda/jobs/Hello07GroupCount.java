package com.loda.jobs;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Author loda
 * @Date 2023/5/2 21:23
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello07GroupCount {
    public static void main(String[] args) throws Exception {
        //env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //拼团主表
        //时间，拼团ID，拼团状态，分类ID
        //1000,p201,1,手机
        //5001,p202,1,手机
        DataStreamSource<String> groupMain = env.socketTextStream("node03", 7777);
        SingleOutputStreamOperator<Tuple4<Long, String, String, String>> groupMainWithWaterMark = groupMain.map(new MapFunction<String, Tuple4<Long, String, String, String>>() {
            @Override
            public Tuple4<Long, String, String, String> map(String value) throws Exception {
                String[] split = value.split(",");
                return Tuple4.of(Long.parseLong(split[0]), split[1], split[2], split[3]);
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple4<Long, String, String, String>>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple4<Long, String, String, String>>() {
                    @Override
                    public long extractTimestamp(Tuple4<Long, String, String, String> element, long recordTimestamp) {
                        return element.f0;
                    }
                }));

        //拼团明细表
        //时间、用户ID、拼团主表ID、订单ID
        //1000,u1646,p201,o1002
        //5001,u1647,p202,o1003
        DataStreamSource<String> groupDetail = env.socketTextStream("node03", 8888);
        SingleOutputStreamOperator<Tuple4<Long, String, String, String>> groupDetailWithWaterMark = groupDetail.map(new MapFunction<String, Tuple4<Long, String, String, String>>() {
            @Override
            public Tuple4<Long, String, String, String> map(String value) throws Exception {
                String[] split = value.split(",");
                return Tuple4.of(Long.parseLong(split[0]), split[1], split[2], split[3]);
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple4<Long, String, String, String>>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple4<Long, String, String, String>>() {
                    @Override
                    public long extractTimestamp(Tuple4<Long, String, String, String> element, long recordTimestamp) {
                        return element.f0;
                    }
                }));

        //订单主表
        //时间，订单ID，订单状态，订单金额
        //1001,o1002,102,200.0
        //5002,o1003,101,3000.0
        DataStreamSource<String> orderMain = env.socketTextStream("node03", 9999);
        SingleOutputStreamOperator<Tuple4<Long, String, String, Double>> orderMainWithWaterMark = orderMain.map(new MapFunction<String, Tuple4<Long, String, String, Double>>() {
            @Override
            public Tuple4<Long, String, String, Double> map(String value) throws Exception {
                String[] split = value.split(",");
                return Tuple4.of(Long.parseLong(split[0]), split[1], split[2], Double.parseDouble(split[3]));
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple4<Long, String, String, Double>>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple4<Long, String, String, Double>>() {
                    @Override
                    public long extractTimestamp(Tuple4<Long, String, String, Double> element, long recordTimestamp) {
                        return element.f0;
                    }
                }));

        //拼团明细表与订单主表coGroup
        DataStream<Tuple2<Tuple4<Long, String, String, String>, Tuple4<Long, String, String, Double>>> joinStream = groupDetailWithWaterMark.coGroup(orderMainWithWaterMark)
                .where(groupDetailData -> groupDetailData.f3)
                .equalTo(orderMainData -> orderMainData.f1)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new CoGroupFunction<Tuple4<Long, String, String, String>, Tuple4<Long, String, String, Double>,
                        Tuple2<Tuple4<Long, String, String, String>, Tuple4<Long, String, String, Double>>>() {
                    @Override
                    public void coGroup(Iterable<Tuple4<Long, String, String, String>> first, Iterable<Tuple4<Long, String, String, Double>> second, Collector<Tuple2<Tuple4<Long, String, String, String>, Tuple4<Long, String, String, Double>>> out) throws Exception {
                        for (Tuple4<Long, String, String, String> groupDetail : first) {
                            boolean isJoined = false;
                            for (Tuple4<Long, String, String, Double> orderMain : second) {
                                isJoined = true;
                                out.collect(Tuple2.of(groupDetail, orderMain));
                            }
                            if (!isJoined) {
                                out.collect(Tuple2.of(groupDetail, null));
                            }
                        }
                    }
                });

        DataStream<Tuple2<Tuple2<Tuple4<Long, String, String, String>, Tuple4<Long, String, String, Double>>, Tuple4<Long, String, String, String>>> res = joinStream.coGroup(groupMainWithWaterMark)
                .where(joinData -> joinData.f0.f2)
                .equalTo(orderMainData -> orderMainData.f1)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new CoGroupFunction<Tuple2<Tuple4<Long, String, String, String>, Tuple4<Long, String, String, Double>>, Tuple4<Long, String, String, String>,
                        Tuple2<Tuple2<Tuple4<Long, String, String, String>, Tuple4<Long, String, String, Double>>, Tuple4<Long, String, String, String>>>() {
                    @Override
                    public void coGroup(Iterable<Tuple2<Tuple4<Long, String, String, String>, Tuple4<Long, String, String, Double>>> first, Iterable<Tuple4<Long, String, String, String>> second,
                                        Collector<Tuple2<Tuple2<Tuple4<Long, String, String, String>, Tuple4<Long, String, String, Double>>, Tuple4<Long, String, String, String>>> out) throws Exception {
                        for (Tuple2<Tuple4<Long, String, String, String>, Tuple4<Long, String, String, Double>> joinData : first) {
                            boolean isJoined = false;
                            for (Tuple4<Long, String, String, String> groupMain : second) {
                                isJoined = true;
                                out.collect(Tuple2.of(joinData, groupMain));
                            }
                            if (!isJoined) {
                                out.collect(Tuple2.of(joinData, null));
                            }
                        }
                    }
                });


        res.print();

        //env exec
        env.execute();
    }
}
