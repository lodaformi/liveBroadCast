package com.loda.udf;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.loda.pojo.DataBean;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author loda
 * @Date 2023/4/26 20:47
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Json2DataBeanV2 extends ProcessFunction<Tuple2<String,String>, DataBean> {

    @Override
    public void processElement(Tuple2<String, String> bean,
                               ProcessFunction<Tuple2<String, String>, DataBean>.Context ctx, Collector<DataBean> out) throws Exception {
        try {
            String id = bean.f0;
            DataBean dataBean = JSON.parseObject(bean.f1, DataBean.class);
            dataBean.setId(id);
            out.collect(dataBean);
        }catch (Exception e) {
            //TODO
        }
    }
}
