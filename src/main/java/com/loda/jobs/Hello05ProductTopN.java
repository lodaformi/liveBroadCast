package com.loda.jobs;

import com.loda.kafka.MyKafkaDeserializationSchema;
import com.loda.pojo.DataBean;
import com.loda.pojo.ItemEventCount;
import com.loda.udf.Hello05GiftTopNAggregationFunction;
import com.loda.udf.Hello05GiftTopNCalcFuntion;
import com.loda.udf.Hello05GiftTopNWindowFunction;
import com.loda.udf.Json2DataBeanV2;
import com.loda.util.FlinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.time.Duration;
import java.util.List;

/**
 * @Author loda
 * @Date 2023/4/29 17:17
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello05ProductTopN {
    public static void main(String[] args) throws Exception {
        //获取kafka中的数据
        DataStream<Tuple2<String, String>> kafkaStream = FlinkUtil.createKafkaStreamV2(args[0], MyKafkaDeserializationSchema.class);
        SingleOutputStreamOperator<DataBean> beanStream = kafkaStream.process(new Json2DataBeanV2());

        //过滤和商品有关的事件（productView（浏览）、productAddCart（加入购物车）、productOrder（下单））
        SingleOutputStreamOperator<DataBean> productStream = beanStream.filter(bean -> bean.getEventId().startsWith("product"));

        //定义水位线和事件时间字段
        SingleOutputStreamOperator<DataBean> beanWithWaterMark = productStream.assignTimestampsAndWatermarks(WatermarkStrategy.<DataBean>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((bean, recordTimestamp) -> bean.getTimestamp()));

        //以eventId、categoryId、productId三元组keyby
        KeyedStream<DataBean, Tuple3<String, String, String>> dataBeanTuple3KeyedStream =
                beanWithWaterMark.keyBy(new KeySelector<DataBean, Tuple3<String, String, String>>() {
                    @Override
                    public Tuple3<String, String, String> getKey(DataBean bean) throws Exception {
                        return Tuple3.of(bean.getEventId(),
                                bean.getProperties().get("categoryId").toString(),
                                bean.getProperties().get("productId").toString());
                    }
                });

        //使用滑动窗口，窗口大小10分钟，每1分钟滑动1次
        WindowedStream<DataBean, Tuple3<String, String, String>, TimeWindow> window =
                dataBeanTuple3KeyedStream.window(SlidingEventTimeWindows.of(Time.minutes(10), Time.minutes(1)));

        // 这里采用aggregate增量聚合，一个商品来了计数+1，
        //process是全量聚合
        // 随后获取eventId，categoryId，商品ID，商品在窗口时间内的次数、窗口开始时间，窗口结束时间，使用ItemEventCount进行包装返回
        SingleOutputStreamOperator<ItemEventCount> aggregate = window.aggregate(new Hello05GiftTopNAggregationFunction(),
                new Hello05GiftTopNWindowFunction());

        //接下来排序
        //将分类ID、事件ID，同一个窗口的数据分到一个分组中
        KeyedStream<ItemEventCount, Tuple4<String, String, Long, Long>> keyedStream = aggregate.keyBy(new KeySelector<ItemEventCount, Tuple4<String, String, Long, Long>>() {
            @Override
            public Tuple4<String, String, Long, Long> getKey(ItemEventCount bean) throws Exception {
                return Tuple4.of(bean.categoryId, bean.eventId, bean.windowStart, bean.windowEnd);
            }
        });

        //将同一个窗口中的商品都添加到一个List<ItemEventCount>中，借助List自带的sort逆序排序，取前三（如果有），返回List<ItemEventCount>
        SingleOutputStreamOperator<List<ItemEventCount>> res = keyedStream.process(new Hello05GiftTopNCalcFuntion());

        res.print("++++++++++++++");

        FlinkUtil.env.execute();
    }
}
