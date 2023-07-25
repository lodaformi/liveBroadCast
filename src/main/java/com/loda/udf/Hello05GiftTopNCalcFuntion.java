package com.loda.udf;

import com.loda.pojo.ItemEventCount;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * @Author loda
 * @Date 2023/4/29 23:25
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello05GiftTopNCalcFuntion extends KeyedProcessFunction<Tuple4<String, String, Long, Long>, ItemEventCount, List<ItemEventCount>> {
    private transient ValueState<List<ItemEventCount>> listValueState;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<List<ItemEventCount>> listValueStateDescriptor =
                new ValueStateDescriptor<>("item-list-state", TypeInformation.of(new TypeHint<List<ItemEventCount>>() {
                }));
        listValueState = getRuntimeContext().getState(listValueStateDescriptor);
    }

    @Override
    public void processElement(ItemEventCount value, KeyedProcessFunction<Tuple4<String, String, Long, Long>, ItemEventCount, List<ItemEventCount>>.Context ctx, Collector<List<ItemEventCount>> out) throws Exception {
        List<ItemEventCount> lst = listValueState.value();
        if (lst == null) {
            lst = new ArrayList<>();
        }
        lst.add(value);
        listValueState.update(lst);

        //使用事件时间注册定时器
        ctx.timerService().registerEventTimeTimer(value.windowEnd + 1);
    }


    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<Tuple4<String, String, Long, Long>, ItemEventCount, List<ItemEventCount>>.OnTimerContext ctx, Collector<List<ItemEventCount>> out) throws Exception {
        //排序
        List<ItemEventCount> lst = listValueState.value();

        lst.sort((o1, o2) -> Long.compare(o2.count, o1.count));

        ArrayList<ItemEventCount> sortedList = new ArrayList<>();
        for (int i = 0; i < Math.min(3, lst.size()); i++) {
            sortedList.add(lst.get(i));
        }
        out.collect(sortedList);
    }
}
