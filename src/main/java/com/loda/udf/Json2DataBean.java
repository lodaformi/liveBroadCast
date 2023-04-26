package com.loda.udf;

import com.alibaba.fastjson.JSON;
import com.loda.pojo.DataBean;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author loda
 * @Date 2023/4/25 19:48
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Json2DataBean extends ProcessFunction<String, DataBean> {

    @Override
    public void processElement(String value, ProcessFunction<String, DataBean>.Context ctx, Collector<DataBean> out) throws Exception {
        try { //防止在转换过程中出现异常
            DataBean dataBean = JSON.parseObject(value, DataBean.class);
            out.collect(dataBean);
        }catch (Exception e) {
            //TODO 异常处理
            System.out.println("----------------Json2DataBean convert error------------");
        }
    }
}
