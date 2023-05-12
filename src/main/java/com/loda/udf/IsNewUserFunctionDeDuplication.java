package com.loda.udf;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.Funnels;
import com.loda.pojo.DataBean;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;

import java.util.Collections;

/**
 * @Author loda
 * @Date 2023/4/26 16:10
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class IsNewUserFunctionDeDuplication extends RichMapFunction<DataBean, DataBean> implements CheckpointedFunction {

    private transient ListState<BloomFilter<String>> listState;
    private transient BloomFilter<String> bloomFilter;

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor listStateDescriptor =
                new ListStateDescriptor("deviceId-bloomFilter-list", TypeInformation.of(new TypeHint<BloomFilter<String>>() {}));
        listState = context.getOperatorStateStore().getListState(listStateDescriptor);

        //初始化布隆过滤器，从列表中拿到布隆过滤器
        if (context.isRestored()){
            for (BloomFilter<String> filter : listState.get()) {
                this.bloomFilter = filter;
            }
        }
    }

    @Override
    public DataBean map(DataBean bean) throws Exception {
        String deviceId = bean.getDeviceId();
        //判断过滤器是否存在
        if (bloomFilter == null) {
            bloomFilter = BloomFilter.create(Funnels.unencodedCharsFunnel(), 10000);
        }
        //如果deviceId不在bloomFilter里
        if (!bloomFilter.mightContain(deviceId)) {
            bloomFilter.put(deviceId);
            bean.setIsN(1);
        }
        return bean;
    }

    //更新布隆过滤器
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        listState.update(Collections.singletonList(bloomFilter));
    }
}
