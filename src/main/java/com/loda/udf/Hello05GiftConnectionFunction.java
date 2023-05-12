package com.loda.udf;

import com.loda.pojo.DataBean;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author loda
 * @Date 2023/4/29 16:57
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello05GiftConnectionFunction extends BroadcastProcessFunction<DataBean, Tuple4<Integer, String, Double, Integer>, Tuple3<String,String,Double>> {
    MapStateDescriptor<Integer, Tuple2<String, Double>> mapStateDescriptor;

    public Hello05GiftConnectionFunction(MapStateDescriptor<Integer, Tuple2<String, Double>> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void processElement(DataBean bean, BroadcastProcessFunction<DataBean, Tuple4<Integer, String, Double, Integer>, Tuple3<String, String, Double>>.ReadOnlyContext ctx,
                               Collector<Tuple3<String, String, Double>> out) throws Exception {

        ReadOnlyBroadcastState<Integer, Tuple2<String, Double>> broadcastState = ctx.getBroadcastState(mapStateDescriptor);

        String anchor_id = bean.getProperties().get("anchor_id").toString();
        Integer gift_id = Integer.parseInt(bean.getProperties().get("gift_id").toString());

        //双流match
        Tuple2<String, Double> giftTP = broadcastState.get(gift_id);

        //如果主流中获取的gift_id能在广播变量中匹配到
        if (giftTP != null) {
            //主播id，礼物名，礼物积分
            out.collect(Tuple3.of(anchor_id, giftTP.f0, giftTP.f1));
        }else {
            out.collect(Tuple3.of(anchor_id, gift_id.toString(), null));
        }
    }

    @Override
    public void processBroadcastElement(Tuple4<Integer, String, Double, Integer> value,
                                        BroadcastProcessFunction<DataBean, Tuple4<Integer, String, Double, Integer>, Tuple3<String, String, Double>>.Context ctx,
                                        Collector<Tuple3<String, String, Double>> out) throws Exception {

        BroadcastState<Integer, Tuple2<String, Double>> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        Integer id = value.f0;
        String name = value.f1;
        Double points = value.f2;
        Integer deleted = value.f3;

        //为了简化操作，直接将状态中id的数据删除，
        // 如果deleted是0，表示没有删除，再重新赋值
        // 如果deleted不是0，不用操作
        broadcastState.remove(id);
        if (deleted == 0) {
            broadcastState.put(id, Tuple2.of(name, points));
        }
    }
}
