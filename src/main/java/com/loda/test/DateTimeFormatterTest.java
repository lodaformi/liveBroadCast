package com.loda.test;

import com.loda.kafka.MyKafkaDeserializationSchema;
import com.loda.pojo.DataBean;
import com.loda.udf.Json2DataBeanV2;
import com.loda.udf.LocationFunction;
import com.loda.util.FlinkUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * @Author loda
 * @Date 2023/4/27 10:55
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class DateTimeFormatterTest {
    public static void main(String[] args) throws Exception {
        //        timestamp = 1618020106810
//        timestamp = 1618020106810
        Long timestamp = 1682565505L;
        LocalDateTime dateTime = LocalDateTime.ofEpochSecond(timestamp, 0, ZoneOffset.UTC);
        String format = dateTime.format(DateTimeFormatter.ofPattern("yyyyMMdd-HH"));
//
//                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd-HH");
//                String format = dateFormat.format(new Date(timestamp));

        String[] split = format.split("-");
        System.out.println(split[0] + "--" + split[1]);


    }
}
