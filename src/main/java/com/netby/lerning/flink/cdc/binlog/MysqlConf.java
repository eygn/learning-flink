package com.netby.lerning.flink.cdc.binlog;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * 数据库配置
 **/
@Data
@AllArgsConstructor
public class MysqlConf {
    private String host;
    private int port;
    private String username;
    private String passwd;
}

