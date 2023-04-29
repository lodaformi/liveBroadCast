package com.loda.test;

import java.util.Date;

/**
 * @Author loda
 * @Date 2023/4/29 16:04
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class DateTest {
    public static void main(String[] args) {
        long timeStamp = 0;
        Date date = new Date(timeStamp);
        System.out.println("+++"+date.toString());
    }
}
