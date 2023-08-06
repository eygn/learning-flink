package com.netby.flink.cdc.binlog;

import lombok.Builder;
import lombok.Data;

/**
 * 监听配置信息
 *
 * @author baiyg
 */
@Data
@Builder
public class MysqlConfig {

    private String host;

    private int port;

    private String username;

    private String passwd;

    private String db;

    private String table;

}


