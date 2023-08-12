package com.netby.flink.cdc.binlog;

import cn.hutool.core.thread.NamedThreadFactory;
import cn.hutool.core.util.RandomUtil;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.*;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.kohsuke.args4j.Option;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

import static com.github.shyiko.mysql.binlog.event.EventType.*;


/**
 * @author baiyg
 * @date 2023/1/6 10:48
 **/
@Slf4j
public class BinLogEventListener implements BinaryLogClient.EventListener {

    @Option(name = "-binlog-consume_threads", usage = "the thread num of consumer")
    private int consumerThreads = BinlogConfigContext.consumerThreads;

    private int queenSize = BinlogConfigContext.queueSize;

    @Getter
    private BinaryLogClient client;

    private BlockingQueue<BinLogItem> queue;
    private final ExecutorService consumer;

    /**
     * 存放每张数据表对应的eventHandler
     */
    private Multimap<String, BinLogEventHandler> eventHandlers;

    private Map<String, Map<String, Colum>> dbTableCols;

    private Map<Long, String> dbTableIds = new HashMap<>();

    private Long firstEventTimeStamp;

    private boolean isQueueFull = false;

    /**
     * 监听器初始化
     *
     * @param mysqlConfig
     */
    public BinLogEventListener(MysqlConfig mysqlConfig) {
        BinaryLogClient client = new BinaryLogClient(mysqlConfig.getHost(), mysqlConfig.getPort(), mysqlConfig.getUsername(), mysqlConfig.getPasswd());
        EventDeserializer eventDeserializer = new EventDeserializer();
        //eventDeserializer.setCompatibilityMode(//序列化
        //        EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG,
        //        EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY
        //);
        client.setBinlogFilename(BinlogConfigContext.binlogFileName);
        client.setBinlogPosition(BinlogConfigContext.binlogPosition);
        client.setEventDeserializer(eventDeserializer);
        // 每个slave的serverId必须唯一:时间戳+随机6位数
        long serverId = Long.valueOf(String.valueOf(System.currentTimeMillis()) + RandomUtil.randomNumbers(6));
        client.setServerId(serverId);
        client.registerLifecycleListener(new BinLogLifecycleListener());

        this.client = client;

        this.eventHandlers = ArrayListMultimap.create();
        this.dbTableCols = new ConcurrentHashMap<>();
        this.consumer = Executors.newFixedThreadPool(consumerThreads, new NamedThreadFactory("binlog-task-", false));
        this.queue = new ArrayBlockingQueue<>(queenSize);
    }

    /**
     * 监听处理
     *
     * @param event
     */
    @Override
    public void onEvent(Event event) {
        String dbTable = null;
        EventType eventType = event.getHeader().getEventType();
        if (eventType == EventType.TABLE_MAP) {
            TableMapEventData tableData = event.getData();
            String db = tableData.getDatabase();
            String table = tableData.getTable();
            dbTable = BinLogUtils.getdbTable(db, table);
            if (firstEventTimeStamp == null && event.getHeader() != null) {
                firstEventTimeStamp = event.getHeader().getTimestamp();
                log.info("firstEventTimeStamp:{}", firstEventTimeStamp);
            }
            dbTableIds.put(tableData.getTableId(), dbTable);
        } else {
            dbTable = dbTableIds.get(BinLogItem.getTableId(event, eventType));
        }

        // 只处理添加删除更新三种操作
        try {
            if (dbTable != null && dbTableCols.containsKey(dbTable)) {
                if (isWrite(eventType)) {
                    WriteRowsEventData data = event.getData();
                    for (Serializable[] row : data.getRows()) {
                        BinLogItem item = BinLogItem.itemFromInsertOrDeleted(row, dbTableCols.get(dbTable), eventType);
                        item.setDbTable(dbTable);
                        if (itemParamsHandle(event, item) != null) {
                            queue.put(item);
                        }
                    }
                }
                if (isUpdate(eventType)) {
                    UpdateRowsEventData data = event.getData();
                    for (Map.Entry<Serializable[], Serializable[]> row : data.getRows()) {
                        BinLogItem item = BinLogItem.itemFromUpdate(row, dbTableCols.get(dbTable), eventType);
                        item.setDbTable(dbTable);
                        if (itemParamsHandle(event, item) != null) {
                            queue.put(item);
                        }
                    }
                }

                // 数据物理删除忽略掉
             /*   if (isDelete(eventType)) {
                    DeleteRowsEventData data = event.getData();
                    for (Serializable[] row : data.getRows()) {
                        if (dbTableCols.containsKey(dbTable)) {
                            BinLogItem item = BinLogItem.itemFromInsertOrDeleted(row, dbTableCols.get(dbTable), eventType);
                            item.setDbTable(dbTable);
                            queue.add(item);
                        }
                    }
                }*/
            }
        } catch (Exception e) {
            log.error("binlog event exception,eventType:{},eventData:{}", eventType, event.getData(), e);
            DingTalkUtil.sendErrorMsg("binlog添加任务队列异常,任务表：" + dbTable + ",eventType:" + eventType + ",异常原因:" + e.getMessage());
        }

    }

    /**
     * item参数处理
     *
     * @param event
     * @param item
     * @return
     */
    private BinLogItem itemParamsHandle(Event event, BinLogItem item) {
        long timestamp = event.getHeader().getTimestamp();
        // 正常情况下：预估一个小时需要2分钟追平，一个Binlog文件可能4个小时就会切换，大概需要10分钟左右
        if (BinlogConfigContext.binlogTimestamp != null && timestamp < BinlogConfigContext.binlogTimestamp) {
            log.info("当前事件时间小于配置的起始时间，消费自动过滤,eventTimestamp：{}，configTimestamp：{}，position：{}", timestamp, BinlogConfigContext.binlogTimestamp, ((EventHeaderV4) event.getHeader()).getPosition());
            return null;
        }
        item.setBinlogFilename(client.getBinlogFilename());
        item.setBinlogPosition(((EventHeaderV4) event.getHeader()).getPosition());
        item.setBinlogTimestamp(timestamp);

        if (queue.size() > queenSize * 0.8) {
            isQueueFull = true;
            DingTalkUtil.sendErrorMsg("当前Binlog消费队列堆积已超过最大容量的80%,任务表为:" + item.getDbTable());
        } else {
            if (isQueueFull) {
                DingTalkUtil.sendErrorMsg("当前Binlog消费队列堆积已恢复,任务表为:" + item.getDbTable());
            }
            isQueueFull = false;
        }

        return item;
    }

    /**
     * 注册监听
     *
     * @param db           数据库
     * @param table        操作表
     * @param eventHandler 事件处理器
     * @throws Exception
     */
    public void regListener(String db, String table, BinLogEventHandler eventHandler) throws Exception {
        String dbTable = BinLogUtils.getdbTable(db, table);
        // 获取字段集合
        Map<String, Colum> cols = BinLogUtils.getColMap(BinlogConfigContext.mysqlConfig, db, table);
        // 保存字段信息
        dbTableCols.put(dbTable, cols);
        // 保存当前注册的listener
        eventHandlers.put(dbTable, eventHandler);
    }

    /**
     * 开启多线程消费
     *
     * @throws IOException
     */
    public void consume() throws IOException {
        client.registerEventListener(this);

        log.info("系统可用线程数：{}，当前配置消费binlog线程数:{}", Runtime.getRuntime().availableProcessors(), consumerThreads);

        for (int i = 0; i < consumerThreads; i++) {
            consumer.submit(() -> {
                while (true) {
                    if (queue.size() > 0) {
                        try {
                            BinLogItem item = queue.take();
                            eventHandlers.get(item.getDbTable()).forEach(binLogEventHandler -> binLogEventHandler.onEvent(item));
                        } catch (Throwable e) {
                            log.error("binLogListener.consume Exception", e);
                            DingTalkUtil.sendErrorMsg("binLogListener.consume Exception:" + e.getMessage());
                        }
                    } else {
                        Thread.sleep(BinlogConfigContext.queueSleep);
                    }
                }
            });
        }
        client.connect();
    }

    public void close() {
        log.info("开始停止binlog...");
        try {
            client.disconnect();
        } catch (Throwable e) {
            log.error("client disconnect exception", e);
        }

        consumer.shutdown();
    }

}

