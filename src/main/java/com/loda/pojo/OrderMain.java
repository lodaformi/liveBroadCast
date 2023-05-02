package com.loda.pojo;

import java.util.Date;

/**
 * @Author loda
 * @Date 2023/5/1 16:19
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class OrderMain {
    private Integer oid;
    private Date create_time;
    private Double total_money;
    private Integer status;
    private Date update_time;
    private Integer uid;
    private String province;
    //sql操作类型：CREATE/INSERT/UPDATE/DELETE
    private String type;

    public OrderMain() {
    }

    public OrderMain(Integer oid, Date create_time, Double total_money, Integer status, Date update_time, Integer uid, String province, String type) {
        this.oid = oid;
        this.create_time = create_time;
        this.total_money = total_money;
        this.status = status;
        this.update_time = update_time;
        this.uid = uid;
        this.province = province;
        this.type = type;
    }

    /**
     * 获取
     * @return oid
     */
    public Integer getOid() {
        return oid;
    }

    /**
     * 设置
     * @param oid
     */
    public void setOid(Integer oid) {
        this.oid = oid;
    }

    /**
     * 获取
     * @return create_time
     */
    public Date getCreate_time() {
        return create_time;
    }

    /**
     * 设置
     * @param create_time
     */
    public void setCreate_time(Date create_time) {
        this.create_time = create_time;
    }

    /**
     * 获取
     * @return total_money
     */
    public Double getTotal_money() {
        return total_money;
    }

    /**
     * 设置
     * @param total_money
     */
    public void setTotal_money(Double total_money) {
        this.total_money = total_money;
    }

    /**
     * 获取
     * @return status
     */
    public Integer getStatus() {
        return status;
    }

    /**
     * 设置
     * @param status
     */
    public void setStatus(Integer status) {
        this.status = status;
    }

    /**
     * 获取
     * @return update_time
     */
    public Date getUpdate_time() {
        return update_time;
    }

    /**
     * 设置
     * @param update_time
     */
    public void setUpdate_time(Date update_time) {
        this.update_time = update_time;
    }

    /**
     * 获取
     * @return uid
     */
    public Integer getUid() {
        return uid;
    }

    /**
     * 设置
     * @param uid
     */
    public void setUid(Integer uid) {
        this.uid = uid;
    }

    /**
     * 获取
     * @return province
     */
    public String getProvince() {
        return province;
    }

    /**
     * 设置
     * @param province
     */
    public void setProvince(String province) {
        this.province = province;
    }

    /**
     * 获取
     * @return type
     */
    public String getType() {
        return type;
    }

    /**
     * 设置
     * @param type
     */
    public void setType(String type) {
        this.type = type;
    }

    public String toString() {
        return "OrderMain{oid = " + oid + ", create_time = " + create_time + ", total_money = " + total_money + ", status = " + status + ", update_time = " + update_time + ", uid = " + uid + ", province = " + province + ", type = " + type + "}";
    }
}
