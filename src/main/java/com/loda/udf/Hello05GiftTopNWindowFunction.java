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
//需要获取6个值（eventId，categoryId，商品ID，次数、窗口开始时间，窗口结束时间），用tuple6操作没有javaBean对象方便
public class Hello05GiftTopNWindowFunction implements WindowFunction<Long, ItemEventCount, Tuple3<String, String, String>, TimeWindow> {

    //窗口触发后每一组都会调用一次（在窗口内增量聚合后的数据)
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
