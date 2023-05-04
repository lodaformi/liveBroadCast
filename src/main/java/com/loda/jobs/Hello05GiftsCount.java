package com.loda.jobs;

import com.loda.kafka.MyKafkaDeserializationSchema;
import com.loda.pojo.DataBean;
import com.loda.source.MysqlSource;
import com.loda.udf.Hello05GiftConnectionFunction;
import com.loda.udf.Json2DataBeanV2;
import com.loda.util.FlinkUtil;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.List;

/**
 * @Author loda
 * @Date 2023/4/29 14:58
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello05GiftsCount {
    public static void main(String[] args) throws Exception {
//        //env
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        //获取mysql中数据
//        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);
//        tableEnvironment.executeSql(
//                "CREATE TABLE giftTable (\n" +
//                        "  id INT,\n" +
//                        "  name STRING,\n" +
//                        "  point DOUBLE,\n" +
//                        "  deleted INT\n" +
//                        "  PRIMARY KEY (id) NOT ENFORCED\n" +
//                        ") WITH (\n" +
//                        "   'connector' = 'jdbc',\n" +
//                        "   'url' = 'jdbc:mysql://node01:3306/liveBroadcast',\n" +
//                        "   'table-name' = 'tb_live_gift',\n" +
//                        "   'driver' = 'com.mysql.jdbc.Driver',\n" +
//                        "   'username' = 'root',\n" +
//                        "   'password' = '123456'\n" +
//                        ");"
//        );
//
//        tableEnvironment.sqlQuery("select * from giftTable").execute();

        //从mysql中获取数据
        DataStreamSource<Tuple4<Integer, String, Double, Integer>> mysqlSource = FlinkUtil.env.addSource(new MysqlSource());
        //检查数据是否正常读取
//        mysqlSource.print();

        //将mysql中数据放到mapState状态中，key为礼物id，value为礼物的name，points和是否删除deleted
        MapStateDescriptor<Integer, Tuple2<String, Double>> mapStateDescriptor =
                new MapStateDescriptor<>("gift_broadcast-state", Types.INT, TypeInformation.of(new TypeHint<Tuple2<String, Double>>() {}));
        //广播
        BroadcastStream<Tuple4<Integer, String, Double, Integer>> broadcastStream = mysqlSource.broadcast(mapStateDescriptor);

        //获取kafka中的数据
        //将kafka的topic/partition/offset组合成唯一id
        DataStream<Tuple2<String, String>> kafkaStream = FlinkUtil.createKafkaStreamV2(args[0], MyKafkaDeserializationSchema.class);
        SingleOutputStreamOperator<DataBean> beanStream = kafkaStream.process(new Json2DataBeanV2());
        //过滤出直播中的礼物数据
        SingleOutputStreamOperator<DataBean> liveRewardStream = beanStream.filter(bean -> "liveReward".equals(bean.getEventId()));

        //数据流合并，处理
        SingleOutputStreamOperator<Tuple3<String, String, Double>> anchorIdGiftNamePointsStream =
                liveRewardStream.connect(broadcastStream).process(new Hello05GiftConnectionFunction(mapStateDescriptor));

        anchorIdGiftNamePointsStream.keyBy(giftTp -> giftTp.f0).sum(2)
                .print();
        //
        FlinkUtil.env.execute();
    }
}
