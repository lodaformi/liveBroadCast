package com.loda.udf;

import com.loda.pojo.ItemEventCount;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.awt.event.ItemEvent;

/**
 * @Author loda
 * @Date 2023/4/29 23:15
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello05GiftTopNWindowFunction implements WindowFunction<Long, ItemEventCount, Tuple3<String, String, String>, TimeWindow> {

    @Override
    public void apply(Tuple3<String, String, String> tp, TimeWindow window, Iterable<Long> input, Collector<ItemEventCount> out) throws Exception {
        String eventId = tp.f0;
        String categoryId = tp.f1;
        String productId = tp.f2;
        Long count = input.iterator().next();
        long start = window.getStart();
        long end = window.getEnd();
        out.collect(new ItemEventCount(productId,eventId,categoryId,count,start,end));
    }
}
