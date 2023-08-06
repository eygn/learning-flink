package com.netby.flink.cdc.binlog;

/**
 * @author baiyg
 * @date 2023/8/4 11:16
 */
public class BinlogConfigContext {

    public static MysqlConfig mysqlConfig;

    /**
     * binlog 文件名称
     */
    public static String binlogFileName;

    /**
     * binlog 事件位置
     */
    public static long binlogPosition;

    /**
     * 消费时间戳
     */
    public static Long binlogTimestamp;

    /**
     * 本地化时区
     */
    public static Boolean timeZoneLocal = false;

    /**
     * 消费线程数(因为需要实时准确记录binlog消费位点，所以只能单线程消费)
     */
    public static final int consumerThreads = 1;

    /**
     * 全局队列长度
     */
    public static final int queueSize = 10000;

    /**
     * 线程休眠时间
     */
    public static final long queueSleep = 150;

}
