package com.loda.test;

/**
 * @Author loda
 * @Date 2023/4/25 16:19
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
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

//使用布隆过滤器
public class ActivityCount02BloomFilter {
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
        })
        //按照活动ID,事件ID进行keyBy，同一种事件、活动的用户一定会进入一个分区
        .keyBy(new KeySelector<Tuple3<String, String, String>, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> getKey(Tuple3<String, String, String> value) throws Exception {
                return Tuple2.of(value.f1, value.f2);
            }
        })
        //在同一组内进行聚合
        .process(new KeyedProcessFunction<Tuple2<String, String>, Tuple3<String, String, String>, Tuple4<String,String,Integer,Integer>>() {
            private transient ValueState<BloomFilter<String>> uidFilterState;
            private transient ValueState<Integer> uvState;
            private transient ValueState<Integer> pvState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<BloomFilter<String>> bloomFilterValueStateDescriptor =
                        new ValueStateDescriptor<>("uid-filter", TypeInformation.of(new TypeHint<BloomFilter<String>>() {}));
                uidFilterState = getRuntimeContext().getState(bloomFilterValueStateDescriptor);

                ValueStateDescriptor<Integer> uvStateDescriptor =
                        new ValueStateDescriptor<>("uv-state", Types.INT);
                uvState = getRuntimeContext().getState(uvStateDescriptor);

                ValueStateDescriptor<Integer> pvStateDescriptor = new ValueStateDescriptor<>("pv-state", Types.INT);
                pvState = getRuntimeContext().getState(pvStateDescriptor);
            }

            @Override
            public void processElement(Tuple3<String, String, String> value,
                                       KeyedProcessFunction<Tuple2<String, String>, Tuple3<String, String, String>,
                                               Tuple4<String, String, Integer, Integer>>.Context ctx,
                                       Collector<Tuple4<String, String, Integer, Integer>> out) throws Exception {
                BloomFilter<String> filter = uidFilterState.value();
                Integer uv = uvState.value();

                if (filter == null) {
                    filter = BloomFilter.create(Funnels.unencodedCharsFunnel(),10000);
                    uv = 0;
                }

                if (!filter.mightContain(value.f0)) {
                    filter.put(value.f0);
                    uv++;
                }
                uidFilterState.update(filter);
                uvState.update(uv);

                Integer pv = pvState.value();
                if (pv == null) {
                    pv = 0;
                }
                pv++;
                pvState.update(pv);

                out.collect(Tuple4.of(value.f1, value.f2, uv, pv));
            }
        }).print();
        env.execute();
    }
}
