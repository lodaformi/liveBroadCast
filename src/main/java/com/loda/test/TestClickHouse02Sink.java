package com.loda.test;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author loda
 * @Date 2023/4/26 19:48
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class TestClickHouse02Sink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(2);

        //data: id, name, age, birthday, timestamp
        DataStreamSource<String> source = environment.socketTextStream("node03", 9999);

        source.map(value -> {
                    String[] str = value.split(",");
                    return Tuple3.of(Integer.parseInt(str[0]),str[1],str[2]);
                }, Types.TUPLE(Types.INT, Types.STRING, Types.STRING))
                .addSink(JdbcSink.sink(
                "insert into loda.tb_test (id, name, add) values (?,?,?)",
                (ps, t) -> {
                    ps.setInt(1, t.f0);
                    ps.setString(2, t.f1);
                    ps.setString(3, t.f2);
                },
                JdbcExecutionOptions.builder().withBatchSize(2).build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:clickhouse://node01:8123/loda")
                        .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
                        .withUsername("default")
                        .withPassword("123456")
                        .build()));

        //env exec
        environment.execute();
    }
}
