package com.netby.flink.cdc.jdbc;

import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.DateTime;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.thread.NamedThreadFactory;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.netby.flink.cdc.binlog.BinLogItem;
import com.netby.flink.cdc.binlog.BinlogEventConfig;
import com.netby.flink.cdc.binlog.MysqlBinlogListener;
import com.netby.flink.cdc.third.BaseTuple;
import com.netby.flink.cdc.third.DagNode;
import com.zaxxer.hikari.HikariDataSource;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.netby.flink.cdc.jdbc.Constant.*;

/**
 * 历史数据扫描+binlog实时监听组件
 * 先异步启动历史数据扫描任务(根据sql配置扫描)，同时启动binlog监听最新事件，当数据重叠时会自动停止历史数据扫描任务
 *
 * @author baiyg
 * @date 2023/12/2 11:04
 */
@Data
@Slf4j
public class BinlogAndJdbcSourceComponent extends RichSourceFunction<BaseTuple> implements CheckpointedFunction {
    private static final long serialVersionUID = -9057026857986845521L;

    private String jdbcUrl;
    private String selectSentence;
    private int pageSize;
    private int waitTime;
    private int outputColumnSize;
    private DagNode dagNode;
    private String nodeId;
    private Boolean incrNeeded = Boolean.valueOf(false);
    private String primaryKeyFields;
    private List<String> primaryKeyList;
    private String updateTimeField;
    private int primaryKeySub = 1;
    private int updateTimeSub = 2;
    private transient ExecutorService service;
    private ConcurrentHashMap<String, String> currentTable = new ConcurrentHashMap<>();
    private transient HikariDataSource dataSource;

    /**
     * 非增量采集 暂用不上，使用方式为limit语句
     */
    private transient ListState<Map<String, Long>> offsetValueState;

    private transient Map<String, Long> tableOffset;
    /**
     * 增量，使用方式为时间+主键条件，会持久化时间和主键值，暂停后会再次恢复
     */
    private transient ListState<Map<String, Map<String, String>>> tableInfoState;
    private transient Map<String, Map<String, String>> tableInfoMaps;
    private String nodeName;
    private Properties properties;
    private Set<String> tables;
    private List<String> outputColumns;
    private MysqlBinlogListener mysqlBinlogListener;
    private transient Long binLogStartTime;
    private transient Long binLogFirstEventProcessTime;
    private transient Map<String, Long> tableBinlogPosition;
    /**
     * 组件结束
     */
    private volatile boolean canceled;
    /**
     * 最新扫表时间
     */
    private transient Long lastScanProcessTime;
    /**
     * 扫描任务
     */
    private transient boolean scanTable = true;

    public BinlogAndJdbcSourceComponent(DagNode node) {
        this.properties = node.getProperties();
        this.dagNode = node;
        this.nodeId = node.getNodeId();
        this.nodeName = node.getNodeName();
        this.tables = new HashSet<>();
    }

    /**
     * 组件初始化
     *
     * @param parameters The configuration containing the parameters attached to the contract.
     * @throws Exception
     */
    public void open(Configuration parameters) throws Exception {
        info("open params:{},properties:{}", JSON.toJSONString(parameters), JSON.toJSONString(properties));
        String driverName = Constant.JDBC_DRIVER;
        String username = properties.getProperty("username");
        String password = properties.getProperty("password");

        waitTime = Integer.parseInt(StringUtils.isBlank(properties.getProperty("waitTime")) ? DEFAULT_WAIT_TIME : properties.getProperty("waitTime"));
        jdbcUrl = StringUtils.isNotBlank(properties.getProperty("jdbcUrl")) ? properties.getProperty("jdbcUrl") : properties.getProperty("address");
        selectSentence = properties.getProperty("select");
        info(nodeName + "the query sql string is:" + selectSentence);

        tables.add(JdbcUtil.getTablePatternFromSql(selectSentence));

        String incrNeeded = properties.getProperty("incrNeeded");
        if (StringUtils.isNotBlank(incrNeeded) && "yes".equals(incrNeeded)) {
            this.incrNeeded = Boolean.valueOf(true);
            primaryKeyFields = properties.getProperty("primaryKeyColumns");
            if (StringUtils.isNotBlank(primaryKeyFields)) {
                String[] split = primaryKeyFields.split(",");
                primaryKeyList = Arrays.asList(split);
            }
            updateTimeField = properties.getProperty("updateTimeColumn");
            initFieldSub(properties.getProperty("outputColumns").split(","));
        }
        pageSize = Integer.parseInt(StringUtils.isBlank(properties.getProperty("limit")) ? DEFAULT_PAGE_SIZE : properties.getProperty("limit"));
        outputColumns = Arrays.asList(properties.getProperty("outputColumns").split(","));
        outputColumnSize = outputColumns.size();
        canceled = false;

        try {
            dataSource = DBConnectUtil.initDataSource(username, password, driverName, jdbcUrl);
            initTableStatus();
        } catch (InterruptedException e) {
            log.warn(e.getMessage());
            Thread.currentThread().interrupt();
        } catch (Exception se) {
            throw new IllegalArgumentException("open() failed." + se.getMessage(), se);
        }

        String schema = jdbcUrl.split("\\?")[0].split("/")[3];
        String ip = jdbcUrl.split("/")[2];
        String hostname = ip.split(":")[0];
        String port = ip.split(":")[1];
        scanTable = true;
        // 这种场景适合用Executors.newCachedThreadPool来创建线程池
        /*service = new ThreadPoolExecutor(4,
                Runtime.getRuntime().availableProcessors() * 2, 60L, TimeUnit.MILLISECONDS,
                new LinkedBlockingDeque<>(8192), new NamedThreadFactory("scan-history-task-", false), new ThreadPoolExecutor.AbortPolicy() {
            @Override
            public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
                DingTalkUtil.sendErrorMsg("处理历史数据线程池队列已满,任务提交被拒绝,线程数:" + e.getActiveCount() + ",queenSize:" + e.getQueue().size() + ",任务表:" + tables);
                super.rejectedExecution(r, e);
            }
        });*/
        service = Executors.newCachedThreadPool(new NamedThreadFactory("scan-history-task-", false));

        BinlogEventConfig binlogEventConfig = BinlogEventConfig.builder()
                .host(hostname)
                .port(Integer.valueOf(port))
                .username(username)
                .passwd(password)
                .db(schema)
                .table(StringUtils.join(tables.toArray(), ","))
                .build();
        mysqlBinlogListener = new MysqlBinlogListener(binlogEventConfig);
    }

    /**
     * 运行任务
     *
     * @param ctx The context to emit elements to and for accessing locks.
     * @throws Exception
     */
    public void run(SourceContext<BaseTuple> ctx) throws Exception {
        info("run params:{}", JSON.toJSONString(ctx));
        service.execute(() -> {
            try {
                scanHistoryData(ctx);
            } catch (InterruptedException e) {
                log.warn(e.getMessage());
                Thread.currentThread().interrupt();
            } catch (Throwable e) {
                log.error("scanHistoryData exception", e);
            }
            log.info("停止扫描表历史数据，binLogStartTime:{},scanLastTime:{}", DateUtil.format(new Date(binLogStartTime), DatePattern.NORM_DATETIME_MS_PATTERN), DateUtil.format(new Date(lastScanProcessTime), DatePattern.NORM_DATETIME_MS_PATTERN));
        });

        service.execute(() -> {
            try {
                binLogStartTime = System.currentTimeMillis();
                mysqlBinlogListener.regListener(item -> {
                    log.info("接收到binlog事件：{}", item.getAfter());
                    handleBinLogEvent(ctx, item);
                });
            } catch (Throwable e) {
                log.error("run binLog exception", e);
            }
        });

        while (!canceled) {
            TimeUnit.SECONDS.sleep(2);
        }
    }

    /**
     * 处理binlog事件
     *
     * @param ctx
     * @param item
     */
    private void handleBinLogEvent(SourceContext<BaseTuple> ctx, BinLogItem item) {
        Map<String, Serializable> after = item.getAfter();
        BaseTuple tuple = BaseTuple.newInstance(outputColumnSize);
        // 填充tuple
        for (int index = 0; index < outputColumnSize; index++) {
            Object obj = after.get(outputColumns.get(index));
            if (obj != null) {
                if (obj instanceof byte[]) {
                    tuple.setField(index, new String((byte[]) obj));
                } else if (obj instanceof java.sql.Timestamp) {
                    tuple.setField(index, Long.toString(((java.sql.Timestamp) obj).getTime()));
                } else {
                    tuple.setField(index, String.valueOf(obj));
                }
            } else {
                tuple.setField(index, obj);
            }
        }
        Map<String, String> tableInfoMap = new HashMap<>();

        if (incrNeeded.booleanValue()) {
            if (StringUtils.isNotBlank(updateTimeField) && StringUtils.isNotBlank(Objects.toString(after.get(updateTimeField), null))) {
                tableInfoMap.put(primaryKeyFields, Objects.toString(after.get(primaryKeyFields), null));
                tableInfoMap.put(updateTimeField, Objects.toString(after.get(updateTimeField), null));
                lastScanProcessTime = parseTime(Objects.toString(after.get(updateTimeField), null));
            } else if (StringUtils.isBlank(Objects.toString(after.get(updateTimeField), null))) {
                tableInfoMap.put(primaryKeyFields + "NonTime", Objects.toString(after.get(primaryKeyFields), null));
            }
        }
        tuple.setEmitTime(System.currentTimeMillis());
        if (binLogFirstEventProcessTime == null) {
            binLogFirstEventProcessTime = System.currentTimeMillis();
        }
        log.info("binlog collect:{}", JSON.toJSONString(tuple));
        ctx.collect(tuple);

        // 记录状态
        if (incrNeeded.booleanValue()) {
            if (tableInfoMap.size() > 0) {
                tableInfoMaps.put(item.getDbTable(), tableInfoMap);
            }
        }
    }

    /**
     * 扫描历史数据
     *
     * @param ctx
     * @throws InterruptedException
     */
    private void scanHistoryData(SourceContext<BaseTuple> ctx) throws InterruptedException {
        while (scanTable) {
            Map<String, String> selectSentences = new HashMap<>(this.tables.size());
            selectSentences.put(JdbcUtil.getTablePatternFromSql(this.selectSentence), this.selectSentence);
            long startTime = System.currentTimeMillis();

            for (Map.Entry<String, String> entry : selectSentences.entrySet()) {
                String tableName = entry.getKey();
                String selectSql = entry.getValue();
                if (this.incrNeeded.booleanValue()) {
                    String queryStr = buildSql(tableName, selectSql);
                    if (!this.currentTable.containsKey(tableName)) {
                        this.currentTable.put(tableName, tableName);
                        info(this.nodeName + "--mysql--current query string is:" + queryStr);
                        executorSelect(ctx, queryStr, tableName);
                    }
                    continue;
                }
                if (!this.currentTable.containsKey(tableName)) {
                    this.currentTable.put(tableName, tableName);
                    long currentOffset = (this.tableOffset.get(tableName) != null) ? ((Long) this.tableOffset.get(tableName)).longValue() : 0L;
                    String queryStr = String.format("%s %s %s,%s", new Object[]{selectSql, "limit", Long.valueOf(currentOffset), Integer.valueOf(this.pageSize)});
                    info(this.nodeName + "--current query string is:" + queryStr);
                    executorSelect(ctx, queryStr, tableName);
                }
            }

            long sleepTime = this.waitTime + startTime - System.currentTimeMillis();
            if (sleepTime <= 0L) {
                return;
            }
            TimeUnit.MILLISECONDS.sleep(sleepTime);
        }
    }

    /**
     * 构建sql，这里会做多个 union all 拼接
     *
     * @param tableName
     * @param selectSql
     * @return
     */
    private String buildSql(String tableName, String selectSql) {
        log.info("buildSql tableName:{}", tableName);
        String queryStr;
        if (this.tableInfoMaps.containsKey(tableName)) {
            String querySql1, querySql3;
            Map<String, String> tableInfoMap = this.tableInfoMaps.get(tableName);
            String updateTime = "";

            StringBuilder primaryKeySb = new StringBuilder();
            String primaryKeyNontime = "";

            String querySql2 = "";

            if (tableInfoMap.containsKey(this.primaryKeyFields) && tableInfoMap.containsKey(this.updateTimeField)) {
                updateTime = tableInfoMap.get(this.updateTimeField);

                for (int i = 0; i < this.primaryKeyList.size(); i++) {
                    primaryKeySb.append(" and " + (String) this.primaryKeyList.get(i) + " > '" + (String) tableInfoMap.get(this.primaryKeyList.get(i)) + "'");
                }
                String updateFieldStr1 = " and " + this.updateTimeField + " is not null and " + this.updateTimeField + " = '" + updateTime + "'";
                String updateFieldStr2 = " and " + this.updateTimeField + " is not null and " + this.updateTimeField + " > '" + updateTime + "'";

                querySql1 = "select * from (" + selectSql + ") m where 1=1 " + updateFieldStr1 + primaryKeySb.toString();
                querySql2 = "select * from (" + selectSql + ") n where 1=1 " + updateFieldStr2;
            } else {
                querySql1 = "select * from (" + selectSql + ") m where 1=1 and " + this.updateTimeField + " is not null";
            }
            String updateFieldStr3 = " and " + this.updateTimeField + " is null";
            if (tableInfoMap.containsKey(this.primaryKeyFields + "NonTime")) {
                primaryKeyNontime = tableInfoMap.get(this.primaryKeyFields + "NonTime");
                String primaryFieldStr2 = " and " + this.primaryKeyFields + " > '" + primaryKeyNontime + "'";
                querySql3 = "select * from (" + selectSql + ") n where 1=1 " + updateFieldStr3 + primaryFieldStr2;
            } else {
                querySql3 = "select * from (" + selectSql + ") n where 1=1 " + updateFieldStr3;
            }
            if (this.jdbcUrl.contains(MYSQL)) {

                if (!"".equals(updateTime)) {
                    queryStr = "select * from ((" + querySql1 + ") union all (" + querySql2 + ") union all (" + querySql3 + "))s order by " + this.updateTimeField + "," + this.primaryKeyFields + " limit " + this.pageSize;
                } else if (!"".equals(primaryKeyNontime)) {
                    queryStr = "select * from ((" + querySql1 + ") union all (" + querySql3 + "))s order by " + this.updateTimeField + "," + this.primaryKeyFields + " limit " + this.pageSize;
                } else {
                    throw new RuntimeException("构建SQL异常！");
                }

            } else if (!"".equals(updateTime)) {
                queryStr = "select * from ((" + querySql1 + ") union all (" + querySql2 + ") union all (" + querySql3 + ") order by " + this.updateTimeSub + "," + this.primaryKeySub + ")s " + "where rownum<" + (this.pageSize + 1);
            } else if (!"".equals(primaryKeyNontime)) {
                queryStr = "select * from ((" + querySql1 + ") union all (" + querySql3 + ") order by " + this.updateTimeSub + "," + this.primaryKeySub + ")s " + "where rownum<" + (this.pageSize + 1);
            } else {
                throw new RuntimeException("构建SQL异常！");
            }

        } else if (this.jdbcUrl.contains(MYSQL)) {
            queryStr = "select * from (" + selectSql + ")s order by " + this.updateTimeField + "," + this.primaryKeyFields + " limit " + this.pageSize;
        } else {
            queryStr = "select * from (select * from (" + selectSql + ") m order by " + this.updateTimeField + "," + this.primaryKeyFields + ") s " + "where rownum<" + (this.pageSize + 1);
        }

        return queryStr;
    }

    /**
     * 执行sql语句
     *
     * @param ctx
     * @param queryStr
     * @param table
     */
    private void executorSelect(SourceContext<BaseTuple> ctx, String queryStr, String table) {
        info("executorSelect params:{}", JSON.toJSONString(ctx), queryStr, table);
        this.service.execute(() -> {
            ResultSet resultSet = null;

            Statement statement = null;

            Connection dbConn = null;

            try {
                dbConn = this.dataSource.getConnection();

                statement = dbConn.createStatement();

                int resultSize = 0;
                resultSet = statement.executeQuery(queryStr);
                Map<String, String> tableInfoMap = new HashMap<>();
                if (this.tableInfoMaps != null) {
                    for (Map<String, String> map : this.tableInfoMaps.values()) {
                        tableInfoMap = map;
                    }
                }
                while (resultSet != null && resultSet.next()) {
                    if (binLogStartTime != null && StringUtils.isNotBlank(updateTimeField) && StringUtils.isNotBlank(resultSet.getString(this.updateTimeSub))) {
                        lastScanProcessTime = parseTime(resultSet.getString(this.updateTimeSub));
                        if (binLogStartTime < lastScanProcessTime) {
                            log.info("stop scanTable,binLogProcessTime:{},scanProcessTime:{}", binLogStartTime, lastScanProcessTime);
                            scanTable = false;
                            break;
                        }
                    }
                    BaseTuple tuple = BaseTuple.newInstance(this.outputColumnSize);
                    for (int i = 1; i <= this.outputColumnSize; i++) {
                        if (resultSet.getObject(i) instanceof java.sql.Timestamp) {
                            tuple.setField(i - 1, Long.toString(resultSet.getTimestamp(i).getTime()));
                        } else {
                            tuple.setField(i - 1, resultSet.getString(i));
                        }
                    }
                    if (this.incrNeeded.booleanValue()) {
                        if (StringUtils.isNotBlank(resultSet.getString(this.updateTimeSub)) && StringUtils.isNotBlank(resultSet.getString(this.primaryKeySub))) {
                            tableInfoMap.put(this.primaryKeyFields, resultSet.getString(this.primaryKeySub));
                            for (int i = 0; i < this.primaryKeyList.size(); i++) {
                                tableInfoMap.put(this.primaryKeyList.get(i), resultSet.getString(this.primaryKeyList.get(i)));
                            }
                            tableInfoMap.put(this.updateTimeField, resultSet.getString(this.updateTimeSub));
                        } else if (StringUtils.isBlank(resultSet.getString(this.updateTimeSub))) {
                            tableInfoMap.put(this.primaryKeyFields + "NonTime", resultSet.getString(this.primaryKeySub));
                        }
                    }
                    tuple.setEmitTime(System.currentTimeMillis());
                    ctx.collect(tuple);
                    resultSize++;
                }
                if (lastScanProcessTime != null && binLogStartTime != null && binLogStartTime < lastScanProcessTime && scanTable) {
                    log.info("it's time to stop scanTable,binLogProcessTime:{},scanProcessTime:{}", binLogStartTime, lastScanProcessTime);
                    scanTable = false;
                }
                if (!scanTable) {
                    this.currentTable.remove(table);
                    return;
                }
                if (this.incrNeeded.booleanValue()) {
                    if (tableInfoMap.size() > 0) {
                        this.tableInfoMaps.put(table, tableInfoMap);
                    }
                } else if (this.tableOffset.containsKey(table)) {
                    long preOffset = ((Long) this.tableOffset.get(table)).longValue();
                    long currentOffset = preOffset + resultSize;
                    this.tableOffset.put(table, Long.valueOf(currentOffset));
                    info(this.nodeName + "--table[{}] pre offset is[{}],current offset set to [{}]", new Object[]{table, Long.valueOf(preOffset), Long.valueOf(currentOffset)});
                } else {
                    this.tableOffset.put(table, Long.valueOf(resultSize));
                    info(this.nodeName + "--table[{}] pre offset is 0,current offset set to [{}]", table, Integer.valueOf(resultSize));
                }
                this.currentTable.remove(table);
            } catch (Throwable t) {
                this.currentTable.remove(table);
                log.warn(this.nodeName + "--execute query [{}] error.\n{}", new Object[]{queryStr, t.getMessage(), t});
            } finally {
                try {
                    if (resultSet != null) {
                        resultSet.close();
                    }
                    if (statement != null) {
                        statement.close();
                    }
                    if (dbConn != null && !dbConn.isClosed()) {
                        dbConn.close();
                    }
                } catch (SQLException se) {
                    info("Statement couldn't be closed - {}", se.getMessage(), se);
                }
            }
        });
    }

    private long parseTime(String time) {
        try {
            return DateTime.of(time, DatePattern.NORM_DATETIME_MS_PATTERN).getTime();
        } catch (Exception e) {
            return DateTime.of(time, DatePattern.NORM_DATETIME_PATTERN).getTime();
        }
    }

    /**
     * 处理主键和时间列的序号
     *
     * @param outputColumns
     */
    private void initFieldSub(String[] outputColumns) {
        info("initFieldSub params:{}", JSON.toJSONString(outputColumns));
        int i = 1;
        for (String outputColumn : outputColumns) {
            if (outputColumn.trim().toLowerCase().equals(this.primaryKeyFields.trim().toLowerCase())) {
                this.primaryKeySub = i;
            }
            if (outputColumn.trim().toLowerCase().equals(this.updateTimeField.trim().toLowerCase())) {
                this.updateTimeSub = i;
            }
            i++;
        }
    }

    /**
     * 如果恢复时未读取到数据，这里重新做初始化
     *
     * @throws InterruptedException
     */
    private void initTableStatus() throws InterruptedException {
        info(nodeName + "--start to init tables offset");
        boolean ready = false;
        while (!ready) {
            Thread.sleep(3000L);
            if (this.tables != null) {
                ready = true;
            }
        }

        if (this.tableOffset == null && CollectionUtils.isNotEmpty(this.tables)) {
            this.tableOffset = new HashMap<>(this.tables.size());
            info("tables [{}] get from instance {}", String.join(",", (Iterable) this.tables), this.nodeId);
            for (String table : this.tables) {
                if (!this.tableOffset.containsKey(table)) {
                    this.tableOffset.put(table, Long.valueOf(0L));
                    info("table offset of [{}] initial to 0", table);
                }
            }
        }

        if (this.tableInfoMaps == null && CollectionUtils.isNotEmpty(this.tables)) {
            this.tableInfoMaps = new HashMap<>(this.tables.size());
            info("tableInfoMaps Initialized");
        }
    }

    public String getName() {
        return "数据库采集-binlog";
    }

    public String getIcon() {
        return "sql.png";
    }

    public String getMemo() {
        return "从数据库中分页采集数据-binlog";
    }

    public String getType() {
        return MYSQL;
    }


    /**
     * 定期快照备份状态，待重新启动时可用来恢复
     *
     * @param context the context for drawing a snapshot of the operator
     * @throws Exception
     */
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        if (this.canceled) {
            log.warn("snapshotState() called on closed source");
        } else if (this.incrNeeded.booleanValue()) {
            this.tableInfoState.clear();
            this.tableInfoState.add(this.tableInfoMaps);
        } else {
            this.offsetValueState.clear();
            this.offsetValueState.add(this.tableOffset);
        }
    }

    /**
     * 启动时读取恢复状态
     *
     * @param context the context for initializing the operator
     * @throws Exception
     */
    public void initializeState(FunctionInitializationContext context) throws Exception {
        info("initializeState");
        // 初始化表的读取位置 用于limit起始位置  --非增量采集 暂用不上
        this.offsetValueState = context.getOperatorStateStore().getListState(new ListStateDescriptor("offset-states_" + this.nodeId, (TypeInformation) new MapTypeInfo(Types.STRING, Types.LONG)));

        for (Map<String, Long> offsetState : (Iterable<Map<String, Long>>) this.offsetValueState.get()) {
            this.tableOffset = offsetState;
        }

        if (this.tableOffset != null) {
            for (Map.Entry<String, Long> entry : this.tableOffset.entrySet()) {
                info("table[{}] last offset get from last checkpoint is:{}", entry.getKey(), entry.getValue());
            }
        }

        // 记录每个表的读取位置：主键 时间等  --增量采集
        this.tableInfoState = context.getOperatorStateStore().getListState(new ListStateDescriptor("offset-states_Increment_" + this.nodeId, (TypeInformation) new MapTypeInfo(Types.STRING, Types.MAP(Types.STRING, Types.STRING))));
        for (Map<String, Map<String, String>> tableInfo : (Iterable<Map<String, Map<String, String>>>) this.tableInfoState.get()) {
            this.tableInfoMaps = tableInfo;
        }

        if (this.tableInfoMaps != null) {
            for (Map.Entry<String, Map<String, String>> entry : this.tableInfoMaps.entrySet()) {
                info("table[{}] last primaryKey and updateTime get from last checkpoint is:{}", entry.getKey(), JSONObject.toJSONString(entry.getValue()));
            }
        }
    }


    public void cancel() {
        this.canceled = true;
    }

    public void close() throws Exception {
        log.info("开始停止组件");
        cancel();

        try {
            if (this.dataSource != null && !this.dataSource.isClosed()) {
                this.dataSource.close();
            }
        } catch (Exception se) {
            info("Connection couldn't be closed - {}", se.getMessage(), se);
        }
        if (this.service != null) {
            this.service.shutdown();
        }

        if (mysqlBinlogListener.getBinLogEventListener() != null) {
            mysqlBinlogListener.getBinLogEventListener().close();
        }
    }


    /**
     * 自定义打印日志
     *
     * @param format
     * @param arguments
     */
    public void info(String format, Object... arguments) {
        try {
            log.info(format, arguments, SerializerFeature.IgnoreErrorGetter);
        } catch (Throwable e) {
            log.error("日志打印异常", e);
        }
    }


    public BinlogAndJdbcSourceComponent() {
    }
}