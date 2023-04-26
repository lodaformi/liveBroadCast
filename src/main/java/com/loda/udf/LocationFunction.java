package com.loda.udf;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.util.concurrent.AsyncFunction;
import com.loda.pojo.DataBean;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.util.EntityUtils;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Supplier;

/**
 * 作者：WQC
 * 项目：yjxxt-lzj
 * 公司：优极限学堂
 * 部门：大数据
 */
public class LocationFunction extends RichAsyncFunction<DataBean,DataBean> {

    private transient CloseableHttpAsyncClient httpclient; //异步请求的HttpClient
    private String url; //请求高德地图URL地址
    private String key; //请求高德地图的秘钥，注册高德地图开发者后获得
    private int maxConnTotal; //异步HTTPClient支持的最大连接

    public LocationFunction(String url, String key, int maxConnTotal) {
        this.url = url;
        this.key = key;
        this.maxConnTotal = maxConnTotal;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        RequestConfig requestConfig = RequestConfig.custom().build();
        httpclient = HttpAsyncClients.custom() //创建HttpAsyncClients请求连接池
                .setMaxConnTotal(maxConnTotal) //设置最大连接数 默认值100
                .setDefaultRequestConfig(requestConfig).build();
        httpclient.start(); //启动异步请求httpClient
    }


    @Override
    public void asyncInvoke(DataBean bean, ResultFuture<DataBean> resultFuture) throws Exception {
        Double longitude = bean.getLongitude();//获取经度
        Double latitude = bean.getLatitude();//获取纬度
        HttpGet httpGet = new HttpGet(url + "?location=" + longitude + "," + latitude + "&key=" + key);

        Future<HttpResponse> future = httpclient.execute(httpGet, null);

        CompletableFuture.supplyAsync(new Supplier<DataBean>() {
            @Override
            public DataBean get() {
                HttpResponse response = null;
                try {
                    String province = null;
                    String city = null;
                    response = future.get();
                    if (response.getStatusLine().getStatusCode() == 200){
                        //解析返回的结果，获取省份，城市等信息
                        String result = EntityUtils.toString(response.getEntity());
                        JSONObject jsonObject = JSON.parseObject(result);
                        JSONObject regeocode = jsonObject.getJSONObject("regeocode");
                        if(regeocode != null && !regeocode.isEmpty()){
                            JSONObject addressComponent = regeocode.getJSONObject("addressComponent");
                            province = addressComponent.getString("province");
                            city = addressComponent.getString("city");
                        }
                    }

                    bean.setProvince(province);
                    bean.setCity(city);
                    return bean;

                } catch (Exception e) {
                    e.printStackTrace();
                    return null;
                }
            }
        }).thenAccept((DataBean res) -> {
           resultFuture.complete(Collections.singleton(res));
        });
    }

    @Override
    public void close() throws Exception {
        httpclient.close();
    }
}
