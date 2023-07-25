package com.loda.test;

/**
 * @Author loda
 * @Date 2023/7/11 18:57
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Boolean2StringTest {
    public static void main(String[] args) {
        System.out.println(Boolean.parseBoolean("true"));
        System.out.println(Boolean.parseBoolean("abc"));
        System.out.println(Integer.parseInt("123"));
        System.out.println(Integer.valueOf("123"));
        System.out.println(Boolean.getBoolean("abc"));
        System.out.println(Boolean.valueOf("abc"));
        System.out.println(Boolean.valueOf(" "));
    }
}
