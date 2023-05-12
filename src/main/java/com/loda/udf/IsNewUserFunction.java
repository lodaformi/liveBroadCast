package com.loda.udf;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.loda.pojo.DataBean;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;


public class IsNewUserFunction extends KeyedProcessFunction<String, DataBean, DataBean> {
    private transient ValueState<BloomFilter<String>> bloomFilterState;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<BloomFilter<String>> stringValueStateDescriptor =
                new ValueStateDescriptor<>("uid-bloom-filter-state", TypeInformation.of(new TypeHint<BloomFilter<String>>() {}));
        bloomFilterState = getRuntimeContext().getState(stringValueStateDescriptor);
    }

    @Override
    public void processElement(DataBean bean, Context ctx, Collector<DataBean> out) throws Exception {
        String deviceId = bean.getDeviceId();

        BloomFilter<String> bloomFilter = bloomFilterState.value();
        if (bloomFilter == null){
            bloomFilter = BloomFilter.create(Funnels.unencodedCharsFunnel(),10000);
        }

        //使用BloomFilter根据deviceId判断是不是新用户
        if(!bloomFilter.mightContain(deviceId)){
            bloomFilter.put(deviceId);
            bean.setIsN(1);
            bloomFilterState.update(bloomFilter);
        }

        out.collect(bean);
    }
}
