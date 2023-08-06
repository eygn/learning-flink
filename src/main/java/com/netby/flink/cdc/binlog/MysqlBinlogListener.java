package com.netby.flink.cdc.binlog;

import cn.hutool.core.collection.CollectionUtil;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * @author baiyg
 * @date 2023/1/6 10:45
 */
@Data
@Slf4j
@NoArgsConstructor
public class MysqlBinlogListener {

    private MysqlConfig mysqlConfig;

    private BinLogEventListener binLogEventListener;


    public MysqlBinlogListener(MysqlConfig mysqlConfig) {
        this.mysqlConfig = mysqlConfig;
        log.info("初始化binlog配置信息：{}", mysqlConfig.toString());
        // 初始化监听器
        binLogEventListener = new BinLogEventListener(mysqlConfig);
        BinlogConfigContext.mysqlConfig = mysqlConfig;
    }

    public void regListener(BinLogEventHandler listener) {
        // 获取table集合
        List<String> tableList = BinLogUtils.getListByStr(mysqlConfig.getTable());
        if (CollectionUtil.isEmpty(tableList)) {
            log.info("binlog未匹配到任何需要监听的表");
            return;
        }
        log.info("binlog开始监听表,tables:{}", tableList);
        // 注册监听
        tableList.forEach(table -> {
            log.info("注册监听信息，注册DB：" + mysqlConfig.getDb() + "，注册表：" + table);
            try {
                binLogEventListener.regListener(mysqlConfig.getDb(), table, listener);
            } catch (Throwable e) {
                log.error("BinLog监听异常", e);
            }
        });
        // 多线程消费
        try {
            binLogEventListener.consume();
        } catch (Throwable e) {
            log.error("mysqlBinLogListener.consume occur exception", e);
            throw new RuntimeException(e);
        }
    }
}
