package com.netby.flink.cdc.config;

import com.dtflys.forest.annotation.Query;
import com.dtflys.forest.annotation.Request;

/**
 * @author: byg
 * @date: 2023/5/21 10:28
 */
public interface MyNacosClient {

    @Request(
            url = "http://192.168.0.200:8848/nacos/v1/cs/configs?group=DEFAULT_GROUP",
            headers = "Accept: text/plain",connectTimeout = 2000,readTimeout = 2000
    )
    String getConfig(@Query("dataId") String dataId, @Query("tenant") String tenant);
}
