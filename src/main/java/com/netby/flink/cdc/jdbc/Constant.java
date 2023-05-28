package com.netby.flink.cdc.jdbc;

public class Constant {
  static final String TABLE_REGEX_PATTERN = "[\\s\\S]*\\$\\{(.*)\\}[\\s\\S]*";
  
  public static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
  
  static final String DEFAULT_WAIT_TIME = "0";
  
  static final String DEFAULT_PAGE_SIZE = "100";
  
  static final String DEFAULT_COLUMN_SPLIT = ",";
  
  static final String MYSQL = "mysql";
  
  static final String OUTPUT_FIELDS = "outputFields";
  
  static final String BATCH_SIZE = "batchSize";
  
  static final String COLUMN_NAME = "COLUMN_NAME";
  
  static final String DATA_TYPE = "DATA_TYPE";
  
  static final String DEFAULT_BATCH_SIZE = "100";
  
  static final String DYNAMIC_TABLE = "dynamicTable";
  
  static final String JDBC_URL = "jdbcUrl";
  
  static final String LEFT_BRACKET = "{";
  
  static final String RIGHT_BRACKET = "}";
  
  static final String RETRY_NUM = "retryNum";
  
  static final String DEFAULT_RETRY_NUM = "3";
  
  static final String LIMIT = "limit";
  
  static final String WAIT_TIME = "waitTime";
  
  static final String RESET_TYPE = "resetType";
  
  static final String UPPER_BOUND = "where rownum<=";
  
  static final String LOWER_BOUND = "where rownum<";
  
  static final String MINUS = "minus";
  
  static final String NO_SCHEDULER = "-1";
  
  static final String TABLE_PATTERN = "tablePattern";
  
  static final String OFFSETS_STATE_NAME = "offset-states";
  
  static final String SELECT = "select";
  
  static final long FLUSH_DURATION = 10000L;
  
  static final int CONN_TIME_OUT = 1000;
  
  static final String OUTPUT_COLUMNS = "outputColumns";
  
  static final String COMPONENT_OBJECT_NAME = "component";
  
  static final String INCR_NEEDED = "incrNeeded";
  
  static final String PRIMARY_KEY_COLUMNS = "primaryKeyColumns";
  
  static final String UPDATE_TIME_COLUMN = "updateTimeColumn";
}