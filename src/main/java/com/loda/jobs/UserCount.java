package com.loda.jobs;

import com.loda.constant.EventID;
import com.loda.pojo.DataBean;
import com.loda.udf.Json2DataBean;
import com.loda.util.FlinkUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.kafka.common.protocol.types.Field;

import java.io.IOException;

/**
 * @Author loda
 * @Date 2023/4/25 19:44
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class UserCount {
    public static void main(String[] args) throws Exception {
        DataStream<String> kafkaStream = FlinkUtil.createKafkaStream(args[0], SimpleStringSchema.class);

        SingleOutputStreamOperator<DataBean> dataStream = kafkaStream.process(new Json2DataBean());

        SingleOutputStreamOperator<DataBean> filtered = dataStream.filter(bean -> EventID.APP_LAUNCH.equals(bean.getEventId()));
        //此处是按照设备类型来区分新旧用户，做了简化，先理解思路。
        // 不是太合理，无法识别一个设备上的多个用户，同一个用户更换了设备被识别为多个用户的情况
        // 严谨点的可以生产全局唯一id
        // 设备类型，是否是新用户，出现次数为1
        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> keyData = filtered.map(bean -> Tuple3.of(bean.getDeviceType(), bean.getIsNew(), 1),
                Types.TUPLE(Types.STRING, Types.INT, Types.INT));
        //keyData.print();
        // (MEIZU-ML6,1,1)
        // (IPHONE-10,0,1)

        //按照机型和新旧用户分组，算出每种机型新用户有多少人，老用户有多少人
        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> sum = keyData.keyBy(tp -> Tuple2.of(tp.f0, tp.f1), Types.TUPLE(Types.STRING, Types.INT))
                .sum(2);
//        sum.print();
        // (REDMI-6,1,4)
        // (REDMI-6,0,9)

        //新用户有多少人，老用户有多少人
        sum.map(new MapFunction<Tuple3<String, Integer, Integer>, Tuple2<Integer,Integer>>() {
            @Override
            public Tuple2<Integer, Integer> map(Tuple3<String, Integer, Integer> value) throws Exception {
                return Tuple2.of(value.f1, value.f2);
            }
        }).keyBy(t -> t.f0).sum(1).print("total");
        //total:6> (1,100)
        //total:6> (0,543)

        FlinkUtil.env.execute();
    }
}
