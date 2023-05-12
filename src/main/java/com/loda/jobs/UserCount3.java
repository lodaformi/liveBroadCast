package com.loda.jobs;

import com.loda.constant.EventID;
import com.loda.pojo.DataBean;
import com.loda.udf.IsNewUserFunction;
import com.loda.udf.Json2DataBean;
import com.loda.util.FlinkUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

/**
 * @Author loda
 * @Date 2023/4/26 9:40
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 * 使用布隆过滤器判断当前设备ID是不是一个新用户
 */
public class UserCount3 {
        public static void main(String[] args) throws Exception {
            DataStream<String> kafkaStream = FlinkUtil.createKafkaStream(args[0], SimpleStringSchema.class);

            //Transform 解析数据 jsonToDataBean
            SingleOutputStreamOperator<DataBean> dataStream = kafkaStream.process(new Json2DataBean());

            //获取符合条件的数据
            SingleOutputStreamOperator<DataBean> filtered = dataStream.filter(bean -> EventID.APP_LAUNCH.equals(bean.getEventId()));

            //按照手机型号进行KeyBy
            KeyedStream<DataBean, String> keyed = filtered.keyBy(DataBean::getDeviceType);

            //使用布隆过滤器判断当前设备ID是不是一个新用户(isN)
            //自定义类继承extends KeyedProcessFunction，重写对应方法，
            //在open函数中创建keyed state状态，在processElement函数中判断是否存在于布隆过滤器中，不存在则表示是新用户，更新布隆过滤器
            //在第4版中使用算子state进行优化
            keyed.process(new IsNewUserFunction()).print();

            FlinkUtil.env.execute();
        }
}
