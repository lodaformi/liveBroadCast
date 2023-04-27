package com.loda.jobs;

import com.loda.kafka.MyKafkaDeserializationSchema;
import com.loda.pojo.DataBean;
import com.loda.udf.Json2DataBeanV2;
import com.loda.udf.LocationFunction;
import com.loda.util.FlinkUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
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
 * @Date 2023/4/26 20:46
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Data2ClickHouse {
    public static void main(String[] args) throws Exception {

        DataStream<Tuple2<String, String>> kafkaStream = FlinkUtil.createKafkaStreamV2(args[0], MyKafkaDeserializationSchema.class);

        SingleOutputStreamOperator<DataBean> beanStream = kafkaStream.process(new Json2DataBeanV2());

        //使用高德地图AMap将经纬度信息转换为地理位置（逆地理转换）
        String url = FlinkUtil.parameterTool.getRequired("amap.http.url");
        String key = FlinkUtil.parameterTool.getRequired("amap.key");

        SingleOutputStreamOperator<DataBean> dataBeanWithLocation = AsyncDataStream.unorderedWait(
                beanStream, new LocationFunction(url, key, 50), 5, TimeUnit.SECONDS
        );

        dataBeanWithLocation.map(new MapFunction<DataBean, DataBean>() {
            @Override
            public DataBean map(DataBean bean) throws Exception {
                Long timestamp = bean.getTimestamp();
//                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd-HH");
//                LocalDateTime dateTime = LocalDateTime.ofEpochSecond(timestamp, 0, ZoneOffset.UTC);

                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd-HH");
                String format = dateFormat.format(new Date(timestamp));
                String[] split = format.split("-");
                bean.setDate(split[0]);
                bean.setHour(split[1]);
                return bean;
            }
        }).addSink(JdbcSink.sink(
                "insert into loda.tb_user_eventvalues (?,?,?,?,?,?,?,?,?,?,?,?)",
                (ps, t) -> {
                    ps.setString(1, t.getId());
                    ps.setString(2, t.getDeviceId());
                    ps.setString(3, t.getEventId());
                    ps.setInt(4, t.getIsNew());
                    ps.setString(5, t.getOsName());
                    ps.setString(6, t.getProvince());
                    ps.setString(7, t.getReleaseChannel());
                    ps.setString(8, t.getDeviceType());
                    ps.setLong(9, t.getTimestamp());
                    ps.setString(10, t.getDate());
                    ps.setString(11, t.getHour());
                    ps.setLong(12, System.currentTimeMillis()/1000);
                },
                JdbcExecutionOptions
                        .builder()
                        .withBatchSize(FlinkUtil.parameterTool.getInt("clickhouse.batch.size"))
                        .withBatchIntervalMs(FlinkUtil.parameterTool.getLong("clickhouse.batch.interval"))
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:clickhouse://node01:8123/loda")
                        .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
                        .withUsername("default")
                        .withPassword("123456")
                        .build()));


        FlinkUtil.env.execute();
    }
}
