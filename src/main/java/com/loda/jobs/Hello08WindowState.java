package com.loda.jobs;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Map;

/**
 * @Author loda
 * @Date 2023/7/16 20:00
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello08WindowState {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Kafka consumer properties
        // ...

        // Create a Flink Kafka consumer
        // ...

        // Add the Kafka consumer as a data source
        // ...
        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);

        DataStream<Tuple2<String, Long>> categoryCountStream = source.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String order) throws Exception {
                // Assuming the order data is in the format: "category quantity"
                String[] orderData = order.split("-");
                String category = orderData[0];
                Long timeStamp = Long.parseLong(orderData[1]);
                return new Tuple2<>(category, timeStamp);
            }
        });

        DataStream<Tuple2<String, Integer>> resultStream = categoryCountStream
                .keyBy(tp->tp.f0) // Key by category (index 0 in the Tuple2)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String ,Long>>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                return element.f1;
                            }
                        }))
                .process(new CountTest());

        // Print the result to the console (for testing)
        resultStream.print();

        env.execute("Kafka Order Consumer");
    }

    public static class  CountTest extends ProcessFunction<Tuple2<String, Long>, Tuple2<String, Integer>> {
        // Declare the MapState variable to store the count per category
        private MapState<String, Integer> countPerCategory;

        @Override
        public void open(Configuration parameters) throws Exception {
            // Initialize the MapState
            MapStateDescriptor<String, Integer> countStateDescriptor = new MapStateDescriptor<>(
                    "countPerCategory",
                    String.class,
                    Integer.class
            );
            countPerCategory = getRuntimeContext().getMapState(countStateDescriptor);
        }


        @Override
        public void processElement(Tuple2<String, Long> value, ProcessFunction<Tuple2<String, Long>, Tuple2<String, Integer>>.Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
            // Retrieve the current count for the category from the state
            Integer currentCount = countPerCategory.get(value.f0);
            if (currentCount == null) {
                currentCount = 0;
            }

            // Update the count by adding the new quantity
            currentCount += new Long(value.f1).intValue();

            // Update the count in the state
            countPerCategory.put(value.f0, currentCount);

            // Emit the updated count
            out.collect(new Tuple2<>(value.f0, currentCount));
        }
    }

    public static class CountPerCategory extends KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>> {
        // Declare the MapState variable to store the count per category
        private MapState<String, Integer> countPerCategory;

        @Override
        public void open(Configuration parameters) throws Exception {
            // Initialize the MapState
            MapStateDescriptor<String, Integer> countStateDescriptor = new MapStateDescriptor<>(
                    "countPerCategory",
                    String.class,
                    Integer.class
            );
            countPerCategory = getRuntimeContext().getMapState(countStateDescriptor);
        }

        @Override
        public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
            // Retrieve the current count for the category from the state
            Integer currentCount = countPerCategory.get(value.f0);
            if (currentCount == null) {
                currentCount = 0;
            }

            // Update the count by adding the new quantity
            currentCount += value.f1;

            // Update the count in the state
            countPerCategory.put(value.f0, currentCount);

            // Emit the updated count
            out.collect(new Tuple2<>(value.f0, currentCount));
        }
    }
}
