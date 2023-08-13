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
     * mysql gtid复制模式，不依赖binlogFileName和position
     * 值如:4c93b24e-e918-11ea-b696-506b4bff3704:1118119350
     */
    public static String gtid = null;

    /**
     * 零时区转换
     */
    public static Boolean timeZoneZero = false;

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
