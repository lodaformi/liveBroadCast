package com.loda.jobs;

import com.loda.kafka.MyKafkaDeserializationSchema;
import com.loda.pojo.DataBean;
import com.loda.udf.Hello04LiveAudienceProcessFunction;
import com.loda.udf.Hello04LiveAudienceProcessFuntionTimer;
import com.loda.udf.Json2DataBeanV2;
import com.loda.udf.LocationFunction;
import com.loda.util.FlinkUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

/**
 * @Author loda
 * @Date 2023/4/28 14:47
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 * 使用定时器每10s统计直播间的累计uv，pv和在线实时人数，并侧流输出，写入到redis
 * 主流写入到clickhouse
 */
public class Hello04LiveAudienceHotInfoAndMultiDimensions {
    public static void main(String[] args) throws Exception {
        FlinkUtil.env.setParallelism(1);
        //kafka source
        //用kafka的topic/partition/offset组成id，主流存放到clickhouse中用于后续查找最新数据
        DataStream<Tuple2<String, String>> kafkaStream = FlinkUtil.createKafkaStreamV2(args[0], MyKafkaDeserializationSchema.class);

        SingleOutputStreamOperator<DataBean> dataBeanStream = kafkaStream.process(new Json2DataBeanV2());

        //使用高德AMap的逆地理转换将经纬度转成地理位置
        String url = FlinkUtil.parameterTool.get("amap.http.url");
        String key = FlinkUtil.parameterTool.get("amap.key");

        SingleOutputStreamOperator<DataBean> locationStream =
                AsyncDataStream.unorderedWait(dataBeanStream, new LocationFunction(url, key, 50), 5, TimeUnit.SECONDS);

        SingleOutputStreamOperator<DataBean> liveStream = locationStream.filter(bean -> bean.getEventId().startsWith("live"));
//        liveStream.print();

        SingleOutputStreamOperator<DataBean> enterOrLeaveStream = liveStream
                .filter(bean -> "liveEnter".equals(bean.getEventId()) || "liveLeave".equals(bean.getEventId()))
                .name("").uid("");
//        enterOrLeaveStream.print();

        KeyedStream<DataBean, String> anchorStream = enterOrLeaveStream.keyBy(bean -> bean.getProperties().get("anchor_id").toString());

        //优化点：状态太多，设置状态的TTL
        //统计累计观众，使用bloomFilter对用户（deviceId）进行过滤
            //只需要考虑进来，不重复即可
        //统计在线观众，使用bloomFilter对用户（deviceId）进行过滤
            //简单：需要考虑进直播间（人数+1）和出直播间（人数-1）
        Hello04LiveAudienceProcessFuntionTimer processFuntionTimer = new Hello04LiveAudienceProcessFuntionTimer();
        SingleOutputStreamOperator<DataBean> process = anchorStream.process(processFuntionTimer);

        //侧输出到redis，用于观察实时和累计数据，可把数据投放到大屏上
        //redis配置
        FlinkJedisPoolConfig node02Redis = new FlinkJedisPoolConfig.Builder()
                .setHost("node02")
                .setPort(6379)
                .setPassword("123123")
                .setDatabase(2)
                .build();
        process.getSideOutput(processFuntionTimer.getAggOutputTag())
                .addSink(new RedisSink<>(node02Redis, new AudienceCountMapper()));

        //        //主流输出到ClickHouse，主要用于多维分析
//        process.print("=======");

        /**
         * CREATE TABLE tb_anchor_audience_count                    \
         * (                                                        \
         *     `id` String comment '数据唯一id',                    \
         * 	   `anchor_id` String comment '主播id',                        \
         *     `deviceId` String comment '用户ID',                  \
         *     `eventId` String comment '事件ID',                   \
         *     `os` String comment '系统名称',                      \
         *     `province` String comment '省份',                    \
         *     `channel` String comment '下载渠道',                 \
         *     `deviceType` String comment '设备类型',              \
         *     `eventTime` DateTime64 comment '数据中所携带的时间', \
         *     `date` String comment 'eventTime转成YYYYMM格式',     \
         *     `hour` String comment 'eventTime转成HH格式列席',     \
         *     `processTime` DateTime DEFAULT now()                 \
         * )                                                        \
         * ENGINE = ReplacingMergeTree(processTime)                 \
         * PARTITION BY (date, hour)                                \
         * ORDER BY id;
         */

        process.addSink(JdbcSink.sink(
                "insert into loda.tb_anchor_audience_count(id,anchor_id,deviceId,eventId,os," +
                        "province,channel,deviceType,eventTime,date,hour)" +
                        " values (?,?,?,?,?,?,?,?,?,?,?)",
                (ps, t) -> {
                    ps.setString(1, t.getId());
                    ps.setString(2, t.getProperties().get("anchor_id").toString());
                    ps.setString(3, t.getDeviceId());
                    ps.setString(4, t.getEventId());
                    ps.setString(5, t.getOsName());
                    ps.setString(6, t.getProvince());
                    ps.setString(7, t.getReleaseChannel());
                    ps.setString(8, t.getDeviceType());
                    ps.setLong(9, t.getTimestamp());
                    ps.setString(10, t.getDate());
                    ps.setString(11, t.getHour());
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
        //env exec
        FlinkUtil.env.execute();
    }

    public static class AudienceCountMapper implements RedisMapper<Tuple4<String, Integer, Integer, Integer>> {
        // 使用hashset数据结构，如果key不存在，新建
        // 如果key存在，新来的value值覆盖旧值
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "audience-count");
        }

        @Override
        public String getKeyFromData(Tuple4<String, Integer, Integer, Integer> data) {
            return data.f0;
        }

        @Override
        public String getValueFromData(Tuple4<String, Integer, Integer, Integer> data) {
            return data.f1+","+ data.f2+","+ data.f3;
        }
    }
}
