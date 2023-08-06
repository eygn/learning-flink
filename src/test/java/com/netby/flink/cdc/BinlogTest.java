package com.netby.flink.cdc;

import com.alibaba.fastjson.JSON;
import com.dtflys.forest.Forest;
import com.netby.flink.cdc.config.MyNacosClient;
import com.netby.flink.cdc.jdbc.JSONParserUtils;
import com.netby.flink.cdc.third.BaseTuple;
import com.netby.flink.cdc.third.DagNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.functions.util.RuntimeUDFContext;
import org.apache.flink.core.fs.Path;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;

public class BinlogTest {

    public static void main(String[] args) throws Exception {
//        String config = IOUtils.toString(ResourceUtils.getURL("config.json"), Charset.forName("UTF-8"));
        /*ClassPathResource resource = new ClassPathResource("config.json");
        if (!resource.exists()) {
            return;
        }

        InputStream inputStream = resource.getInputStream();
        String configJson = IOUtils.toString(inputStream, Charset.defaultCharset());*/

        MyNacosClient myClient = Forest.client(MyNacosClient.class);

        String configJson = myClient.getConfig("flink-cdc-mysql-collect","dev");

        if (StringUtils.isBlank(configJson)) {
            return;
        }
        Properties properties = JSONParserUtils.jsonToProperties(configJson);
        System.err.println(JSON.toJSONString(properties));
        DagNode node = new DagNode();
        node.setNodeId("1");
        node.setProperties(properties);
        BinlogCdcSourceComponent sourceComponent = new BinlogCdcSourceComponent(node);
        OperatorMetricGroup metrics = null;
        Map<String, Accumulator<?, ?>> accumulators = new HashMap<String, Accumulator<?, ?>>();
        Map<String, Future<Path>> cachedFiles = new HashMap<String, Future<Path>>();
        ExecutionConfig executionConfig = new ExecutionConfig();
        ClassLoader userCodeClassLoader = Thread.currentThread().getContextClassLoader();
        TaskInfo taskInfo = new TaskInfo("my-task", 1, 0, 1, 0);
        RuntimeContext runtimeContext = new RuntimeUDFContext(taskInfo, userCodeClassLoader, executionConfig, cachedFiles, accumulators, metrics);
        sourceComponent.setRuntimeContext(runtimeContext);
        sourceComponent.open(null);
        SourceFunction.SourceContext<BaseTuple> ctx = new SourceFunction.SourceContext<BaseTuple>() {
            @Override
            public void collect(BaseTuple element) {
                System.err.println("collect");
                System.err.println(JSON.toJSONString(element));
            }

            @Override
            public void collectWithTimestamp(BaseTuple element, long timestamp) {
                System.err.println("collectWithTimestamp");
                System.err.println(JSON.toJSONString(element));
            }

            @Override
            public void emitWatermark(Watermark mark) {
                System.err.println("emitWatermark");
            }

            @Override
            public void markAsTemporarilyIdle() {
                System.err.println("markAsTemporarilyIdle");
            }

            @Override
            public Object getCheckpointLock() {
                System.err.println("getCheckpointLock");
                return null;
            }

            @Override
            public void close() {
                System.err.println("close");
            }
        };
        sourceComponent.run(ctx);
    }
}