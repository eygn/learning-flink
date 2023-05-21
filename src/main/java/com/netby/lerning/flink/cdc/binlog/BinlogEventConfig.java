package com.netby.lerning.flink.cdc.binlog;

import lombok.Builder;
import lombok.Data;

/**
 * 监听配置信息
 **/
@Data
@Builder
public class BinlogEventConfig {
    private String host;

    private int port;

    private String username;

    private String passwd;

    private String db;

    private String table;

    /**
     * 消费线程数
     */
    public static final int consumerThreads = 5;

    /**
     * 全局队列长度
     */
    public static final int queueSize = 8192;

    /**
     * 线程休眠时间
     */
    public static final long queueSleep = 800;

}


