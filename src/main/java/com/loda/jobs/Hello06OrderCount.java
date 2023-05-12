package com.loda.jobs;

import com.loda.pojo.OrderDetail;
import com.loda.pojo.OrderMain;
import com.loda.udf.Hello06Json2OrderMain;
import com.loda.udf.Hello06OrderLeftJoinFunction;
import com.loda.udf.Hello06Json2OrderDetail;
import com.loda.util.FlinkUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.*;
import java.time.Duration;

/**
 * @Author loda
 * @Date 2023/5/1 16:00
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 * 双流匹配
 * （左表）orderdetail coGroup （右表）ordermain
 */
public class Hello06OrderCount {
    public static void main(String[] args) throws Exception {
        FlinkUtil.env.setParallelism(1);
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(args[0]);

        //canal采集main数据到kafka，flink从kafka对应main主题中读取数据
        String ordermainTopic = parameterTool.getRequired("kafka.input.main");
        DataStream<String> ordermainStream = FlinkUtil.createKafkaStream(parameterTool, ordermainTopic, SimpleStringSchema.class);

        //将数据转换为javaBean
        SingleOutputStreamOperator<OrderMain> orderMainBeanStream = ordermainStream.process(new Hello06Json2OrderMain());

        //canal采集detail数据到kafka，flink从kafka对应detail主题中读取数据
        String orderdetailTopic = parameterTool.getRequired("kafka.input.detail");
        DataStream<String> orderdetailStream = FlinkUtil.createKafkaStream(parameterTool, orderdetailTopic, SimpleStringSchema.class);

        //将数据转换为javaBean
        SingleOutputStreamOperator<OrderDetail> orderDetailBeanStream = orderdetailStream.process(new Hello06Json2OrderDetail());

//        orderMainBeanStream.print();
//        orderDetailBeanStream.print();

        //按照EventTime定义水位线
        SingleOutputStreamOperator<OrderMain> orderMainWithWaterMark = orderMainBeanStream
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderMain>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderMain>() {
                    @Override
                    public long extractTimestamp(OrderMain element, long recordTimestamp) {
                        return element.getUpdate_time().getTime();
                    }
                }));

        SingleOutputStreamOperator<OrderDetail> orderDetailWithWaterMark = orderDetailBeanStream
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderDetail>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
                    @Override
                    public long extractTimestamp(OrderDetail element, long recordTimestamp) {
                        return element.getUpdate_time().getTime();
                    }
                }));

        //定义outputtag用于接收侧输出
        OutputTag<OrderDetail> orderDetailOutputTag = new OutputTag<OrderDetail>("late-order-detail-tag") {};

        //数据迟到解决方法
        //解决方法一：修改源码（效率高）
        //解决方法二：两个窗口，前一个窗口获取迟到数据，后一个窗口做join（窗口的长度和窗口类型与后面join的一致）
        //如果订单明细数据，进入前面的窗口内迟到了，那也说明进入后面的窗口肯定也会迟到，将迟到的数据放到orderDetailOutputTag中
        SingleOutputStreamOperator<OrderDetail> orderDetailWindowStream = orderDetailWithWaterMark.keyBy(bean -> bean.getOrder_id())
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sideOutputLateData(orderDetailOutputTag)
                //apply什么也不错，象征性的收集一下
                .apply(new WindowFunction<OrderDetail, OrderDetail, Integer, TimeWindow>() {
                    @Override
                    public void apply(Integer integer, TimeWindow window, Iterable<OrderDetail> input, Collector<OrderDetail> out) throws Exception {
                        for (OrderDetail orderDetail : input) {
                            out.collect(orderDetail);
                        }
                    }
                });

        //获取OrderDetail迟到数据
        DataStream<OrderDetail> lateOrderDetailStream = orderDetailWindowStream.getSideOutput(orderDetailOutputTag);
        //调整数据格式，以便能与后续join之后的数据union到一起
        //union的基本条件就是要合并的流必须是相同数据类型
        SingleOutputStreamOperator<Tuple2<OrderDetail, OrderMain>> lateOrderDetailFormatStream = lateOrderDetailStream.map(new MapFunction<OrderDetail, Tuple2<OrderDetail, OrderMain>>() {
            @Override
            public Tuple2<OrderDetail, OrderMain> map(OrderDetail value) throws Exception {
                return Tuple2.of(value, null);
            }
        });

        //双流匹配coGroup（全外联，实际上由于orderDetail有侧输出，很大程度上保证左边的表有数据，可理解为left join），OrderDetail做左表
        DataStream<Tuple2<OrderDetail, OrderMain>> joinedStream = orderDetailWithWaterMark.coGroup(orderMainWithWaterMark)
                .where(detailBean -> detailBean.getOrder_id())
                .equalTo(mainBean -> mainBean.getOid())
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new Hello06OrderLeftJoinFunction());

        //将coGroup的流和迟到的流，union到一起，使用相同的方式进行处理
        DataStream<Tuple2<OrderDetail, OrderMain>> unionStream = joinedStream.union(lateOrderDetailFormatStream);

        //将没有关联上左表的右表（ordermain）数据，根据oid去数据库中查询并关联左表数据
        SingleOutputStreamOperator<Tuple2<OrderDetail, OrderMain>> res = unionStream.map(new MapFunction<Tuple2<OrderDetail, OrderMain>, Tuple2<OrderDetail, OrderMain>>() {
            @Override
            public Tuple2<OrderDetail, OrderMain> map(Tuple2<OrderDetail, OrderMain> value) throws Exception {
                if (value.f1 == null) {
                    System.out.println("Hello06OrderCount.map value.f1 == null");
                    OrderMain orderMain = queryOrderMainByOrderId(value.f1.getOid());
                    value.f1 = orderMain;
                }
                return value;
            }
        });

        //可将数据写入到ClickHouse（做多维度分析）
        res.print("+++++++");

        FlinkUtil.env.execute();
    }

    public static OrderMain queryOrderMainByOrderId(Integer orderId) throws SQLException {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        OrderMain orderMain = null;
        try {
            //创建数据库连接
            connection = DriverManager.getConnection("jdbc:mysql://node01:3306/liveBroadcast?characterEncoding=utf-8", "root", "123456");
            String sql = "select total_money, status, uid, province from ordermain where oid = ? ";
            preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setInt(1, orderId);
            //查询数据库
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                orderMain = new OrderMain();

                orderMain.setTotal_money(resultSet.getDouble("total_money"));
                orderMain.setStatus(resultSet.getInt("status"));
                orderMain.setUid(resultSet.getInt("uid"));
                orderMain.setProvince(resultSet.getString("province"));
            }
            resultSet.close();
        } catch (Exception e) {
            //TODO
        } finally {
            preparedStatement.close();
            connection.close();
        }
        //返回结果
        return orderMain;
    }
}
