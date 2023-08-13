## flink组件

- binlog数据采集组件

mysql-binlog 常用的命令

```sql

show master status;

show slave hosts;

show slave status;

show  variables like '%gtid%';

#查看mysql版本
SELECT VERSION();

show variables like '%expire_logs%';

set global binlog_expire_logs_seconds=300;
# 切换文件
flush logs;
#只查看第一个binlog文件的内容
show binlog events;
#获取binlog文件列表
show binary logs;


purge binary logs to 'binlog.000008';
      

```

时区相关
```sql
show variables like '%time_zone%';

SELECT @@global.time_zone;


SET GLOBAL time_zone = '+8:00';
flush privileges;
SET GLOBAL time_zone = '+0:00';
flush privileges;
```