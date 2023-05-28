package com.netby.flink.cdc.jdbc;

import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.SQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DBConnectUtil
{
  private static final Logger log = LoggerFactory.getLogger(DBConnectUtil.class);
  
  private static DBConnectUtil instance;
  
  private HikariDataSource hikariDataSource = new HikariDataSource();
  
  private static final DBConnectUtil getInstance() {
    if (instance == null) {
      instance = new DBConnectUtil();
    }
    return instance;
  }
  
  public final synchronized Connection getConnection() throws SQLException {
    return this.hikariDataSource.getConnection();
  }
  
  public static void commit(Connection conn) {
    if (conn != null) {
      try {
        conn.commit();
      } catch (SQLException e) {
        log.error("提交事务失败,Connection:" + conn);
        e.printStackTrace();
      } finally {
        close(conn);
      } 
    }
  }
  
  public static void rollback(Connection conn) {
    if (conn != null) {
      try {
        conn.rollback();
      } catch (SQLException e) {
        log.error("事务回滚失败,Connection:" + conn);
        e.printStackTrace();
      } finally {
        close(conn);
      } 
    }
  }
  
  public static void close(Connection conn) {
    if (conn != null) {
      try {
        conn.close();
      } catch (SQLException e) {
        log.error("关闭连接失败,Connection:" + conn);
        e.printStackTrace();
      } 
    }
  }
  
  public static HikariDataSource initDataSource(String username, String password, String driverName, String jdbcUrl) {
    HikariDataSource dataSource = new HikariDataSource();
    appendTimeZone(jdbcUrl);
    dataSource.setJdbcUrl(jdbcUrl);
    dataSource.setUsername(username);
    dataSource.setPassword(password);
    dataSource.setMinimumIdle(1);
    dataSource.setMaximumPoolSize(30);
    dataSource.setDriverClassName(driverName);
    dataSource.setAutoCommit(false);
    
    dataSource.setMaxLifetime(480000L);
    dataSource.setIdleTimeout(300000L);
    return dataSource;
  }
  
  public static void appendTimeZone(String jdbcUrl) {
    if (!jdbcUrl.contains("serverTimezone")) {
      if (jdbcUrl.indexOf("?") > 1) {
        jdbcUrl = jdbcUrl + "&serverTimezone=GMT%2B8";
      } else {
        jdbcUrl = jdbcUrl + "?serverTimezone=GMT%2B8";
      }
    }
  }
}