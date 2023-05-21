package com.netby.lerning.flink.cdc.binlog;


/**
 * BinLogListener监听器
 * @author baiyg
 * @date 2023/2/2 11:29
 **/
@FunctionalInterface
public interface BinLogEventHandler {

    /**
     * 监听逻辑处理
     */
    void onEvent(BinLogItem item);
}


