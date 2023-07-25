package com.loda.test;

/**
 * @Author loda
 * @Date 2023/4/25 15:14
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */

import com.loda.util.FlinkUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;

/**
 * 数据样例
 * u1,A1,view
 * u1,A1,view
 * u1,A1,view
 * u1,A1,join
 * u1,A1,join
 * u2,A1,view
 * u2,A1,join
 * u1,A2,view
 * u1,A2,view
 * u1,A2,join
 * <p>
 * 浏览次数：A1,view,4
 * 浏览人数：A1,view,2
 * <p>
 * 参与次数：A1,join,3
 * 参与人数：A1,join,2
 */

//使用hashSet对用户去重
public class ActivityCount01HashSet {
    public static void main(String[] args) throws Exception {
        //env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //source
        DataStreamSource<String> source = env.socketTextStream("node03", 9999);

        source.map(new MapFunction<String, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(String value) throws Exception {
                String[] split = value.split(",");
                return Tuple3.of(split[0], split[1], split[2]);
            }
        }).keyBy(new KeySelector<Tuple3<String, String, String>, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> getKey(Tuple3<String, String, String> value) throws Exception {
                return Tuple2.of(value.f1, value.f2);
            }
        }).process(new KeyedProcessFunction<Tuple2<String, String>, Tuple3<String, String, String>, Tuple4<String, String, Integer, Integer>>() {
            //用户判断用户的是否存在
            private transient ValueState<HashSet<String>> uidState;
            //用户统计用户的数量
            private transient ValueState<Integer> uidCountState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<HashSet<String>> uidStateDescriptor =
                        new ValueStateDescriptor<>("uids-state", TypeInformation.of(new TypeHint<HashSet<String>>() {}));
                uidState = getRuntimeContext().getState(uidStateDescriptor);

                ValueStateDescriptor<Integer> uidCountStateDescriptor = new ValueStateDescriptor<>("uid-countState", Types.INT);
                uidCountState = getRuntimeContext().getState(uidCountStateDescriptor);
            }

            @Override
            public void processElement(Tuple3<String, String, String> value, KeyedProcessFunction<Tuple2<String, String>,
                    Tuple3<String, String, String>, Tuple4<String, String, Integer, Integer>>.Context ctx,
                                       Collector<Tuple4<String, String, Integer, Integer>> out) throws Exception {
                //取出uid
                String uid = value.f0;

                //取出uidSet
                HashSet<String> uidSet = uidState.value();
                if (uidSet == null) {
                    uidSet = new HashSet<>();
                }

                if (!uidSet.contains(uid)) {
                    uidSet.add(uid);
                    uidState.update(uidSet);
                }

                Integer count = uidCountState.value();
                if (count == null) {
                    count = 0;
                }
                count++;
                uidCountState.update(count);

                out.collect(Tuple4.of(value.f1, value.f2, uidSet.size(),count));
            }
        }).print();

        env.execute();
    }
}
