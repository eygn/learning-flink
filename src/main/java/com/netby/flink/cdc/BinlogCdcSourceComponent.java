package com.netby.flink.cdc;

import cn.hutool.core.thread.NamedThreadFactory;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.netby.flink.cdc.binlog.*;
import com.netby.flink.cdc.jdbc.Constant;
import com.netby.flink.cdc.jdbc.DBConnectUtil;
import com.netby.flink.cdc.third.BaseTuple;
import com.netby.flink.cdc.third.DagNode;
import com.zaxxer.hikari.HikariDataSource;
import lombok.Data;
import lombok.NoArgsConstructor;
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
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * binlog实时监听组件
 *
 * @author baiyg
 * @date 2023/12/2 11:04
 */
@Data
@Slf4j
@NoArgsConstructor
public class BinlogCdcSourceComponent extends RichSourceFunction<BaseTuple> implements CheckpointedFunction {

    private static final long serialVersionUID = -9057026857986845521L;

    private static final String BINLOG_TIMEZONE_LOCAL = "timezoneLocal";
    private static final String BINLOG_FILE_NAME = "binlogFileName";
    private static final String BINLOG_POSITION = "binlogPosition";
    private static final String BINLOG_TIMESTAMP = "binlogTimestamp";

    private DagNode dagNode;
    private String nodeId;
    private String nodeName;
    private transient HikariDataSource dataSource;
    private ExecutorService service;
    private Properties properties;
    /**
     * 增量，使用方式为时间+主键条件，会持久化时间和主键值，暂停后会再次恢复
     */
    private transient ListState<Map<String, Map<String, String>>> tableInfoState;
    private transient Map<String, Map<String, String>> tableInfoMaps;

    private String jdbcUrl;
    private String primaryKeyFields;
    private String updateTimeField;
    private String tableName;

    private Set<String> tables;
    private List<String> primaryKeyList;
    private List<String> outputColumns;

    private MysqlBinlogListener mysqlBinlogListener;

    /**
     * 组件结束
     */
    private volatile boolean canceled;

    public BinlogCdcSourceComponent(DagNode node) {
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

        jdbcUrl = StringUtils.isNotBlank(properties.getProperty("jdbcUrl")) ? properties.getProperty("jdbcUrl") : properties.getProperty("address");
        tableName = properties.getProperty("tableName");
        info(nodeName + " the table is:" + tableName);
        tables.add(tableName);

        primaryKeyFields = properties.getProperty("primaryKeyColumns");
        if (StringUtils.isNotBlank(primaryKeyFields)) {
            String[] split = primaryKeyFields.split(",");
            primaryKeyList = Arrays.asList(split);
        }
        updateTimeField = properties.getProperty("updateTimeColumn");

        BinlogConfigContext.timeZoneLocal = Boolean.valueOf(properties.getProperty(BINLOG_TIMEZONE_LOCAL, "false"));
//        if (BinlogConfigContext.timeZoneLocal) {
//            log.info("调整时区，原时区：{}，新时区：{}", TimeZone.getDefault(), TimeZone.getTimeZone("UTC"));
//            TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
//        }
        // 时间戳暂不对外放开(如果要重新推送流立方，则使用原有数据采集组件)
        BinlogConfigContext.binlogTimestamp = Long.valueOf(properties.getProperty(BINLOG_TIMESTAMP, "0"));
        outputColumns = Arrays.asList(properties.getProperty("outputColumns").split(","));
        canceled = false;

        try {
            dataSource = DBConnectUtil.initDataSource(username, password, driverName, jdbcUrl);
            initTableStatus();
        } catch (InterruptedException e) {
            log.error(e.getMessage());
            Thread.currentThread().interrupt();
        } catch (Exception se) {
            throw new IllegalArgumentException("open() failed." + se.getMessage(), se);
        }

        service = Executors.newFixedThreadPool(1, new NamedThreadFactory("binlog-task-", false));

        String schema = jdbcUrl.split("\\?")[0].split("/")[3];
        String ip = jdbcUrl.split("/")[2];
        String hostname = ip.split(":")[0];
        String port = ip.split(":")[1];

        MysqlConfig mysqlConfig = MysqlConfig.builder()
                .host(hostname).port(Integer.valueOf(port)).username(username).passwd(password)
                .db(schema).table(tableName)
                .build();
        log.info("cacheConfig:{}", JSON.toJSONString(this.tableInfoMaps));
        Map<String, String> cacheConfig = this.tableInfoMaps.get(BinLogUtils.getdbTable(schema, tableName));
        if (cacheConfig != null && cacheConfig.size() > 0) {
            String binlogFileName = cacheConfig.get(BINLOG_FILE_NAME);
            long binlogPosition = Long.valueOf(Objects.toString(cacheConfig.get(BINLOG_POSITION), "0"));
            BinlogConfigContext.binlogFileName = binlogFileName;
            BinlogConfigContext.binlogPosition = binlogPosition;
        }
        mysqlBinlogListener = new MysqlBinlogListener(mysqlConfig);
    }

    /**
     * 运行任务
     *
     * @param ctx The context to emit elements to and for accessing locks.
     * @throws Exception
     */
    public void run(SourceContext<BaseTuple> ctx) throws Exception {
        info("run params:{}", JSON.toJSONString(ctx));
        service.submit(() -> {
            try {
                mysqlBinlogListener.regListener(item -> {
                    log.info("接收到binlog事件：{},binlogFileName:{},binlogPosition:{}", item.getAfter(), item.getBinlogFilename(), item.getBinlogPosition());
                    handleBinLogEvent(ctx, item);
                });
            } catch (Throwable e) {
                log.error("run binLog exception", e);
                canceled = true;
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
        BaseTuple tuple = BaseTuple.newInstance(outputColumns.size());
        // 填充tuple
        for (int index = 0; index < outputColumns.size(); index++) {
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


        if (StringUtils.isNotBlank(updateTimeField) && StringUtils.isNotBlank(Objects.toString(after.get(updateTimeField), null))) {
            tableInfoMap.put(primaryKeyFields, Objects.toString(after.get(primaryKeyFields), null));
            tableInfoMap.put(updateTimeField, Objects.toString(after.get(updateTimeField), null));
        } else if (StringUtils.isBlank(Objects.toString(after.get(updateTimeField), null))) {
            tableInfoMap.put(primaryKeyFields + "NonTime", Objects.toString(after.get(primaryKeyFields), null));
        }

        tuple.setEmitTime(System.currentTimeMillis());
        // 记录binlog信息
        tableInfoMap.put(BINLOG_FILE_NAME, item.getBinlogFilename());
        tableInfoMap.put(BINLOG_POSITION, Objects.toString(item.getBinlogPosition(), null));
        tableInfoMap.put(BINLOG_TIMESTAMP, Objects.toString(item.getBinlogTimestamp(), "0"));

        log.info("binlog collect:{}", JSON.toJSONString(tuple));
        ctx.collect(tuple);

        // 记录状态
        if (tableInfoMap.size() > 0) {
            tableInfoMaps.put(item.getDbTable(), tableInfoMap);
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
        return "从数据库中采集数据-binlog";
    }

    public String getType() {
        return Constant.MYSQL;
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
        } else {
            this.tableInfoState.clear();
            this.tableInfoState.add(this.tableInfoMaps);
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

}