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
    private int consumerThreads = BinlogEventConfig.consumerThreads;

    private int queenSize = BinlogEventConfig.queueSize;

    @Getter
    private BinaryLogClient client;

    private BlockingQueue<BinLogItem> queue;
    private final ExecutorService consumer;

    /**
     * 存放每张数据表对应的eventHandler
     */
    private Multimap<String, BinLogEventHandler> eventHandlers;

    private MysqlConf mysqlConf;
    private Map<String, Map<String, Colum>> dbTableCols;
    private String dbTable;

    /**
     * 监听器初始化
     *
     * @param mysqlConf
     */
    public BinLogEventListener(MysqlConf mysqlConf) {
        BinaryLogClient client = new BinaryLogClient(mysqlConf.getHost(), mysqlConf.getPort(), mysqlConf.getUsername(), mysqlConf.getPasswd());
        EventDeserializer eventDeserializer = new EventDeserializer();
        //eventDeserializer.setCompatibilityMode(//序列化
        //        EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG,
        //        EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY
        //);
        // client.setBinlogPosition();
        client.setEventDeserializer(eventDeserializer);
        // 每个slave的serverId必须唯一:时间戳+随机6位数
        long serverId = Long.valueOf(String.valueOf(System.currentTimeMillis()) + RandomUtil.randomNumbers(6));
        client.setServerId(serverId);
        this.client = client;

        this.mysqlConf = mysqlConf;
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
        EventType eventType = event.getHeader().getEventType();

        if (eventType == EventType.TABLE_MAP) {
            TableMapEventData tableData = event.getData();
            String db = tableData.getDatabase();
            String table = tableData.getTable();
            dbTable = BinLogUtils.getdbTable(db, table);
        }

        // 只处理添加删除更新三种操作
        try {
            if (isWrite(eventType) || isUpdate(eventType) || isDelete(eventType)) {
                if (isWrite(eventType)) {
                    WriteRowsEventData data = event.getData();
                    for (Serializable[] row : data.getRows()) {
                        if (dbTableCols.containsKey(dbTable)) {
                            BinLogItem item = BinLogItem.itemFromInsertOrDeleted(row, dbTableCols.get(dbTable), eventType);
                            item.setDbTable(dbTable);
                            queue.add(item);
                        }
                    }
                }
                if (isUpdate(eventType)) {
                    UpdateRowsEventData data = event.getData();
                    for (Map.Entry<Serializable[], Serializable[]> row : data.getRows()) {
                        if (dbTableCols.containsKey(dbTable)) {
                            BinLogItem item = BinLogItem.itemFromUpdate(row, dbTableCols.get(dbTable), eventType);
                            item.setDbTable(dbTable);
                            queue.add(item);
                        }
                    }
                }
                // 数据物理删除忽略掉
                /*if (isDelete(eventType)) {
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
        } catch (IllegalStateException e) {
            DingTalkUtil.sendErrorMsg("binlog消费异常:队列已超过最大值" + queenSize + ",任务表：" + dbTable);
            throw e;
        }
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
        Map<String, Colum> cols = BinLogUtils.getColMap(mysqlConf, db, table);
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

        // 消费异常考虑 大批量更新

        for (int i = 0; i < consumerThreads; i++) {
            consumer.submit(() -> {
                while (true) {
                    if (queue.size() > 0) {
                        try {
                            BinLogItem item = queue.take();
                            eventHandlers.get(item.getDbTable()).forEach(binLogEventHandler -> binLogEventHandler.onEvent(item));
                        } catch (Throwable e) {
                            log.error("binLogListener.onEvent InterruptedException", e);
                            DingTalkUtil.sendErrorMsg("binLogListener.onEvent InterruptedException");
                        }
                    }
                    if (queue.size() > queenSize * 0.8) {
                        DingTalkUtil.sendErrorMsg("当前Binlog消费队列堆积已超过最大容量的80%,实际为:" + queue.size() + ",任务表为:" + dbTable);
                    }
                    Thread.sleep(BinlogEventConfig.queueSleep);
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

