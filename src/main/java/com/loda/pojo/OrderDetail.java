package com.loda.pojo;

import java.util.Date;

/**
 * @Author loda
 * @Date 2023/5/1 16:22
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class OrderDetail {
    private Integer id;
    private Integer order_id;
    private Integer category_id;
    private Integer sku;
    private Double money;
    private Integer amount;
    private Date create_time;
    private Date update_time;
    //sql操作类型：CREATE/INSERT/UPDATE/DELETE
    private String type;


    public OrderDetail() {
    }

    public OrderDetail(Integer id, Integer order_id, Integer category_id, Integer sku, Double money, Integer amount, Date create_time, Date update_time, String type) {
        this.id = id;
        this.order_id = order_id;
        this.category_id = category_id;
        this.sku = sku;
        this.money = money;
        this.amount = amount;
        this.create_time = create_time;
        this.update_time = update_time;
        this.type = type;
    }

    /**
     * 获取
     * @return id
     */
    public Integer getId() {
        return id;
    }

    /**
     * 设置
     * @param id
     */
    public void setId(Integer id) {
        this.id = id;
    }

    /**
     * 获取
     * @return order_id
     */
    public Integer getOrder_id() {
        return order_id;
    }

    /**
     * 设置
     * @param order_id
     */
    public void setOrder_id(Integer order_id) {
        this.order_id = order_id;
    }

    /**
     * 获取
     * @return category_id
     */
    public Integer getCategory_id() {
        return category_id;
    }

    /**
     * 设置
     * @param category_id
     */
    public void setCategory_id(Integer category_id) {
        this.category_id = category_id;
    }

    /**
     * 获取
     * @return sku
     */
    public Integer getSku() {
        return sku;
    }

    /**
     * 设置
     * @param sku
     */
    public void setSku(Integer sku) {
        this.sku = sku;
    }

    /**
     * 获取
     * @return money
     */
    public Double getMoney() {
        return money;
    }

    /**
     * 设置
     * @param money
     */
    public void setMoney(Double money) {
        this.money = money;
    }

    /**
     * 获取
     * @return amount
     */
    public Integer getAmount() {
        return amount;
    }

    /**
     * 设置
     * @param amount
     */
    public void setAmount(Integer amount) {
        this.amount = amount;
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
        return "OrderDetail{id = " + id + ", order_id = " + order_id + ", category_id = " + category_id + ", sku = " + sku + ", money = " + money + ", amount = " + amount + ", create_time = " + create_time + ", update_time = " + update_time + ", type = " + type + "}";
    }
}
