package com.loda.udf;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.loda.pojo.OrderMain;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author loda
 * @Date 2023/5/1 16:25
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello06Json2OrderMain extends ProcessFunction<String, OrderMain> {
    @Override
    public void processElement(String value, ProcessFunction<String, OrderMain>.Context ctx, Collector<OrderMain> out) throws Exception {
        try {
            JSONObject jsonObject = JSON.parseObject(value);
            String type = jsonObject.getString("type");
            if ("INSERT".equals(type) || "UPDATE".equals(type) || "DELETE".equals(type)) {
                JSONArray array = jsonObject.getJSONArray("data");
                for (int i = 0; i < array.size(); i++) {
                    OrderMain orderMain = array.getObject(i, OrderMain.class);
                    orderMain.setType(type);
                    out.collect(orderMain);
                }
            }
        }catch (Exception e) {
            //TODO
        }
    }
}
