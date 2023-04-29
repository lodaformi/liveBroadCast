package com.loda.source;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.*;

/**
 * @Author loda
 * @Date 2023/4/29 15:24
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class MysqlSource extends RichSourceFunction<Tuple4<Integer, String, Double, Integer>> {
    private Connection connection = null;
    PreparedStatement preparedStatement = null;
    private Boolean flag = true;

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = DriverManager.getConnection("jdbc:mysql://node01:3306/liveBroadcast?characterEncoding=utf-8", "root", "123456");
    }

    @Override
    public void run(SourceContext<Tuple4<Integer, String, Double, Integer>> ctx) throws Exception {
        long timeStamp = 0;
        while (flag) {
            String sql = "select id, name, points, deleted from tb_live_gift where updateTime > ? " +(timeStamp == 0 ? "AND deleted = 0" : "");
            preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setDate(1, new Date(timeStamp));
            ResultSet resultSet = preparedStatement.executeQuery();
            timeStamp = System.currentTimeMillis();
            while (resultSet.next()) {
                int id = resultSet.getInt("id");
                String name = resultSet.getString("name");
                double points = resultSet.getDouble("points");
                int deleted = resultSet.getInt("deleted");
                ctx.collect(Tuple4.of(id, name, points, deleted));
            }
            resultSet.close();
            Thread.sleep(10000);
        }
    }

    //什么时候触发？？？
    @Override
    public void cancel() {
        flag = false;
    }

    @Override
    public void close() throws Exception {
        preparedStatement.close();
        connection.close();
    }
}
