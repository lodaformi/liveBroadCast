package com.loda.jobs;

import com.loda.constant.EventID;
import com.loda.pojo.DataBean;
import com.loda.udf.IsNewUserFunctionDeDuplication;
import com.loda.udf.Json2DataBean;
import com.loda.udf.LocationFunction;
import com.loda.util.FlinkUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

/**
 * @Author loda
 * @Date 2023/4/26 15:23
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */

//题目：统计每个省份的新老用户，按设备类型分组，并对设备id进行去重
//优化点：使用算子state，一个taskSlot一个bloomFilter
public class UserCount4 {
    public static void main(String[] args) throws Exception {
        //kafkaStream
        DataStream<String> kafkaStream = FlinkUtil.createKafkaStream(args[0], SimpleStringSchema.class);

        //将json数据转化为javaBean，方便操作
        SingleOutputStreamOperator<DataBean> json2DataBean = kafkaStream.process(new Json2DataBean());

        //
        SingleOutputStreamOperator<DataBean> filtered = json2DataBean.filter(bean -> EventID.APP_LAUNCH.equals(bean.getEventId()));

//        //使用AMap将用户的经纬度转化为省市区
//        String url = FlinkUtil.parameterTool.getRequired("amap.http.url");
//        String key = FlinkUtil.parameterTool.getRequired("amap.key");
//        SingleOutputStreamOperator<DataBean> dataBeanWithLocation = AsyncDataStream.unorderedWait(filtered, new LocationFunction(url, key, 50), 5, TimeUnit.SECONDS);

        //使用算子state，一个并行度共享算子state，节省内存
        //extends RichMapFunction<DataBean, DataBean> implements CheckpointedFunction
        //initializeState中创建布隆过滤器，在map中判断数据是否存在布隆过滤器中，在snapshotState中更新布隆过滤器

        //以什么作为键，看业务需求
        filtered.keyBy(DataBean::getReleaseChannel)
                .map(new IsNewUserFunctionDeDuplication())
                .print();

        //env exec
        FlinkUtil.env.execute();
    }
}
