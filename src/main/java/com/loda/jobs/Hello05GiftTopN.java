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
public class Hello05GiftTopN {
    public static void main(String[] args) throws Exception {
        //获取kafka中的数据
        DataStream<Tuple2<String, String>> kafkaStream = FlinkUtil.createKafkaStreamV2(args[0], MyKafkaDeserializationSchema.class);
        SingleOutputStreamOperator<DataBean> beanStream = kafkaStream.process(new Json2DataBeanV2());

        //过滤和商品有关的事件（productView（浏览）、productAddCart：（加入购物车）、productOrder（下单））
        SingleOutputStreamOperator<DataBean> productStream = beanStream.filter(bean -> bean.getEventId().startsWith("product"));

        SingleOutputStreamOperator<DataBean> beanWithWaterMark = productStream.assignTimestampsAndWatermarks(WatermarkStrategy.<DataBean>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((bean, recordTimestamp) -> bean.getTimestamp()));

        KeyedStream<DataBean, Tuple3<String, String, String>> dataBeanTuple3KeyedStream =
                beanWithWaterMark.keyBy(new KeySelector<DataBean, Tuple3<String, String, String>>() {
                    @Override
                    public Tuple3<String, String, String> getKey(DataBean bean) throws Exception {
                        return Tuple3.of(bean.getEventId(),
                                bean.getProperties().get("categoryId").toString(),
                                bean.getProperties().get("productId").toString());
                    }
                });

        //划分窗口
        WindowedStream<DataBean, Tuple3<String, String, String>, TimeWindow> window = dataBeanTuple3KeyedStream.window(SlidingEventTimeWindows.of(Time.minutes(10), Time.minutes(1)));

        //增量聚合
        //eventId，categoryId，商品ID，次数、窗口开始时间，窗口结束时间
        SingleOutputStreamOperator<ItemEventCount> aggregate = window.aggregate(new Hello05GiftTopNAggregationFunction(),
                new Hello05GiftTopNWindowFunction());

        //接下来排序
        //将分类ID、事件ID，同一个窗口的数据分到一个分组中
        KeyedStream<ItemEventCount, Tuple4<String, String, Long, Long>> keyedStream = aggregate.keyBy(new KeySelector<ItemEventCount, Tuple4<String, String, Long, Long>>() {
            @Override
            public Tuple4<String, String, Long, Long> getKey(ItemEventCount value) throws Exception {
                return Tuple4.of(value.categoryId, value.eventId, value.windowStart, value.windowEnd);
            }
        });

        SingleOutputStreamOperator<List<ItemEventCount>> res = keyedStream.process(new Hello05GiftTopNCalcFuntion());

        res.print("++++++++++++++");

        FlinkUtil.env.execute();
    }
}
