package com.loda.udf;

import com.loda.pojo.DataBean;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichAggregateFunction;
import org.apache.flink.api.common.functions.RichMapFunction;

/**
 * @Author loda
 * @Date 2023/4/29 23:09
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello05GiftTopNAggregationFunction implements AggregateFunction<DataBean, Long, Long> {
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    //来一条执行一条
    @Override
    public Long add(DataBean value, Long accumulator) {
        return accumulator+1;
    }

    //窗口触发时执行
    @Override
    public Long getResult(Long accumulator) {
        return accumulator;
    }

    @Override
    public Long merge(Long a, Long b) {
        return null;
    }

}
