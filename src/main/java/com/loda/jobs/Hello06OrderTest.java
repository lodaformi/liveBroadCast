package com.loda.jobs;

import com.loda.pojo.OrderMain;
import com.loda.udf.Hello06Json2OrderMain;
import com.loda.util.FlinkUtil;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

/**
 * @Author loda
 * @Date 2023/5/12 19:36
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello06OrderTest {
    public static void main(String[] args) throws Exception {
        FlinkUtil.env.setParallelism(1);
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(args[0]);

        //canal采集main数据到kafka，flink从kafka对应main主题中读取数据
        String ordermainTopic = parameterTool.getRequired("kafka.input.main");
        DataStream<String> ordermainStream = FlinkUtil.createKafkaStream(parameterTool, ordermainTopic, SimpleStringSchema.class);
        ordermainStream.print();

        //将数据转换为javaBean
//        SingleOutputStreamOperator<OrderMain> orderMainBeanStream = ordermainStream.process(new Hello06Json2OrderMain());
        FlinkUtil.env.execute();
    }
}
