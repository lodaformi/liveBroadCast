package com.loda.jobs;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author loda
 * @Date 2023/5/12 20:52
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello06MapTest {
    public static void main(String[] args) throws Exception {
        //env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //source

        //迭代次数
        int iterativeNum = 10;
//        DataSet<Integer> wordList = env.fromElements(1, 2, 3);
        DataStreamSource<Integer> wordList = env.fromElements(1, 2, 3);

        IterativeStream<Integer> iterate = wordList.iterate(iterativeNum);

//        IterativeDataSet<Integer> iterativeDataSet=wordList.iterate(iterativeNum);

//        iterativeDataSet.map(new MapFunction<Integer, Object>() {
//            @Override
//            public Object map(Integer value) throws Exception {
//                return null;
//            }
//        });
//
//        iterativeDataSet.mapPartition(new MapPartitionFunction<Integer, Object>() {
//            @Override
//            public void mapPartition(Iterable<Integer> values, Collector<Object> out) throws Exception {
//
//            }
//        });




        env.execute();
    }
}
