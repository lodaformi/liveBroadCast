package com.loda.jobs;

import com.loda.constant.EventID;
import com.loda.pojo.DataBean;
import com.loda.udf.Json2DataBean;
import com.loda.udf.LocationFunction;
import com.loda.util.FlinkUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.util.concurrent.TimeUnit;

/**
 * @Author loda
 * @Date 2023/4/25 23:00
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class UserCount2 {
    public static void main(String[] args) throws Exception {
        DataStream<String> kafkaStream = FlinkUtil.createKafkaStream(args[0], SimpleStringSchema.class);

        SingleOutputStreamOperator<DataBean> dataStream = kafkaStream.process(new Json2DataBean());

        SingleOutputStreamOperator<DataBean> filtered = dataStream.filter(bean -> EventID.APP_LAUNCH.equals(bean.getEventId()));

        String url = FlinkUtil.parameterTool.getRequired("amap.http.url");
        String key = FlinkUtil.parameterTool.getRequired("amap.key");

        //使用高德AMap逆地理编码，异步将经纬度解析为省市
        SingleOutputStreamOperator<DataBean> dataBeanWithLocation = AsyncDataStream.unorderedWait(
                filtered, new LocationFunction(url, key, 50), 5, TimeUnit.SECONDS
        );
        //dataBeanWithLocation.print();

        //每个省，新老用户，出现次数为1
        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> locationUser = dataBeanWithLocation.map(new MapFunction<DataBean, Tuple3<String, Integer, Integer>>() {
            @Override
            public Tuple3<String, Integer, Integer> map(DataBean bean) throws Exception {
                return Tuple3.of(bean.getProvince(), bean.getIsNew(), 1);
            }
        })
        //按照省份和新老用户keyby，统计每个省新用户数量，老用户数量
        .keyBy(new KeySelector<Tuple3<String, Integer, Integer>, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> getKey(Tuple3<String, Integer, Integer> value) throws Exception {
                return Tuple2.of(value.f0, value.f1);
            }
        }).sum(2);

        locationUser.print();

        FlinkUtil.env.execute();
    }
}
