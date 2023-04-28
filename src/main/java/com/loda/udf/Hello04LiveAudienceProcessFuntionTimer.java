package com.loda.udf;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.loda.pojo.DataBean;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;

/**
 * @Author loda
 * @Date 2023/4/28 16:33
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello04LiveAudienceProcessFuntionTimer extends KeyedProcessFunction<String, DataBean, DataBean> {
    private transient ValueState<Integer> pvState;
    private transient ValueState<Integer> uvState;
    private transient ValueState<Integer> onlineUserstate;
    private transient ValueState<BloomFilter<String>> bloomFilterValueState;

    private SimpleDateFormat dateTimeFormatter = new SimpleDateFormat("yyyyMMdd-HH");

    //在线观众使用侧输出
    private OutputTag<Tuple4<String, Integer, Integer, Integer>> aggOutputTag =
            new OutputTag("anchorId-uv-pv-onlineUser", Types.TUPLE(Types.STRING, Types.INT, Types.INT, Types.INT)) {};

    @Override
    public void open(Configuration parameters) throws Exception {
        //设置状态的TTL
        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.hours(6))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .build();

        //4个状态
        ValueStateDescriptor<Integer> pvStateDesc = new ValueStateDescriptor<Integer>("pv-state", Types.INT);
        pvStateDesc.enableTimeToLive(ttlConfig);
        pvState = getRuntimeContext().getState(pvStateDesc);

        ValueStateDescriptor<Integer> uvStateDesc = new ValueStateDescriptor<Integer>("uv-state", Types.INT);
        uvStateDesc.enableTimeToLive(ttlConfig);
        uvState = getRuntimeContext().getState(uvStateDesc);

        ValueStateDescriptor<Integer> onlineUserStateDesc = new ValueStateDescriptor<Integer>("onlineUser-state", Types.INT);
        onlineUserStateDesc.enableTimeToLive(ttlConfig);
        onlineUserstate = getRuntimeContext().getState(onlineUserStateDesc);

        ValueStateDescriptor<BloomFilter<String>> bloomFilterValueStateDescriptor =
                new ValueStateDescriptor<>("bloomFilter-state", TypeInformation.of(new TypeHint<BloomFilter<String>>() {
                }));
        bloomFilterValueStateDescriptor.enableTimeToLive(ttlConfig);
        bloomFilterValueState = getRuntimeContext().getState(bloomFilterValueStateDescriptor);
    }

    //优化点：
    // 状态太多，设置状态的TTL
    // 使用定时器
    // 将在线人数的数据放到侧输出

    //统计pv，进入直播间，计数+1
    //统计累计观众uv，使用bloomFilter对用户（deviceId）进行过滤
    //只需要考虑进来，不重复即可
    //统计在线观众onlineUser，使用bloomFilter对用户（deviceId）进行过滤
    //简单需求：需要考虑进直播间（人数+1）和出直播间（人数-1）
    //需要使用定时器
    @Override
    public void processElement(DataBean bean, KeyedProcessFunction<String, DataBean,
            DataBean>.Context ctx, Collector<DataBean> out)
            throws Exception {
        String deviceId = bean.getDeviceId();
        BloomFilter<String> bloomFilter = bloomFilterValueState.value();
        Integer pv = pvState.value();
        Integer uv = uvState.value();
        Integer onlineUser = onlineUserstate.value();

        //记录当前时间
        long currentProcessingTime = ctx.timerService().currentProcessingTime();
        //当前时间+10s为触发器生效的时间，在10S之内的数据都会被计算到，不管数据是10S内的那个时间达到的
        long fireTime = currentProcessingTime - currentProcessingTime % 100 + 100;
        //注册时间触发器
        ctx.timerService().registerProcessingTimeTimer(fireTime);

        if (onlineUser == null) {
            onlineUser = 0;
        }

        //当观众进入直播间
        if ("liveEnter".equals(bean.getEventId())) {
            if (bloomFilter == null) {
                bloomFilter = BloomFilter.create(Funnels.unencodedCharsFunnel(), 10000);
                pv = 0;
                uv = 0;
            }

            //在线用户不能放到bloomFilter中进行判断，因为当用户推出直播间，再次进入时，
            // 在线用户需要+1，但是放到bloomFilter中就会造成用户大概率已经存在，在线用户统计错误的情况
            if (!bloomFilter.mightContain(deviceId)) {
                bloomFilter.put(deviceId);
                uvState.update(++uv);
                bloomFilterValueState.update(bloomFilter);
            }
            onlineUserstate.update(++onlineUser);
            pvState.update(++pv);
        } else {
            //当观众退出直播间
            onlineUserstate.update(--onlineUser);
        }


        String dateHour = dateTimeFormatter.format(bean.getTimestamp());
        String[] dataHourArr = dateHour.split("-");
        bean.setDate(dataHourArr[0]);
        bean.setHour(dataHourArr[1]);

    //out.collect(Tuple4.of(ctx.getCurrentKey(), uv, pv, onlineUser));
        //主流输出
        out.collect(bean);
    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<String, DataBean, DataBean>.
            OnTimerContext ctx, Collector<DataBean> out) throws Exception {

        String dateHour = dateTimeFormatter.format(timestamp);
        String[] dataHourArr = dateHour.split("-");

        ctx.output(aggOutputTag,
                Tuple4.of(ctx.getCurrentKey() + "_" + dataHourArr[0],
                        uvState.value(),
                        pvState.value(),
                        onlineUserstate.value()));
    }

    public OutputTag<Tuple4<String, Integer, Integer, Integer>> getAggOutputTag() {
        return aggOutputTag;
    }
}
