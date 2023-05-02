package com.loda.udf;

import com.loda.pojo.OrderDetail;
import com.loda.pojo.OrderMain;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @Author loda
 * @Date 2023/5/1 19:26
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello06OrderLeftJoinFunction implements CoGroupFunction<OrderDetail, OrderMain, Tuple2<OrderDetail, OrderMain>> {
    @Override
    public void coGroup(Iterable<OrderDetail> orderDetails, Iterable<OrderMain> orderMains,
                        Collector<Tuple2<OrderDetail, OrderMain>> out) throws Exception {

        for (OrderDetail orderDetail : orderDetails) {
            boolean isJoined = false;
            for (OrderMain orderMain : orderMains) {
                if (orderMain != null) {
                    isJoined = true;
                    out.collect(Tuple2.of(orderDetail, orderMain));
                }
            }
            if (!isJoined) {
                out.collect(Tuple2.of(orderDetail, null));
            }
        }
    }
}
