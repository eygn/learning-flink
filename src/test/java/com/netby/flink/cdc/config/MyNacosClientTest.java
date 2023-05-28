package com.netby.flink.cdc.config;


import com.dtflys.forest.Forest;

/**
 * @author: byg
 * @date: 2023/5/21 10:30
 */
class MyNacosClientTest {

    public static void main(String[] args) {
        MyNacosClient myClient = Forest.client(MyNacosClient.class);

        System.err.println( myClient.getConfig("flink-cdc-mysql-collect","dev"));
    }
}