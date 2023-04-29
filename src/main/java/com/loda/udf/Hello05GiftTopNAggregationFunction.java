package com.loda.udf;

import com.loda.pojo.DataBean;
import org.apache.flink.api.common.functions.AggregateFunction;

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

    @Override
    public Long add(DataBean value, Long accumulator) {
        return accumulator+1;
    }

    @Override
    public Long getResult(Long accumulator) {
        return accumulator;
    }

    @Override
    public Long merge(Long a, Long b) {
        return null;
    }
}
