package com.netby.lerning.flink.cdc.jdbc;/*
package cn.com.bsfit.pipeace.apis.component.metrics;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

public class JobMetrics implements Serializable {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    private static final long serialVersionUID = -238046821678008085L;
    transient MetricGroup paMetrics;
    private String nodeId;
    private transient Counter inputDataNum;
    private transient Counter processDelay;
    private transient Counter translateDelay;
    private transient Meter meter60;
    private transient Meter meter600;
    private transient Meter meter3600;
    private transient Meter meter86400;
    private transient Counter outputDataNum;
    private transient Counter dirtyDataNum;
    public static final String INPUT_DATA_NUM_SUFFIX = "_inputDataNum";
    public static final String OUTPUT_DATA_NUM_SUFFIX = "_outputDataNum";
    public static final String DIRTY_DATA_NUM_SUFFIX = "_dirtyNum";
    public static final String TRANS_DELAY_SUFFIX = "_trans_delay";
    public static final String PROCESS_DELAY_SUFFIX = "_process_delay";
    public static final String ONE_MINUTE_SUFFIX = "_60s";
    public static final String TEN_MINUTE_SUFFIX = "_600s";
    public static final String ONE_HOUR_SUFFIX = "_3600s";
    public static final String ONE_DAY_SUFFIX = "_86400s";
    public static final String PA_USER_DEFINED_METRIC_GROUP = "Pa";

    public JobMetrics(String nodeId, RuntimeContext context) {
        try {
            this.paMetrics = context.getMetricGroup().addGroup(PA_USER_DEFINED_METRIC_GROUP);
        } catch (Throwable e) {
            log.error("JobMetrics occur exception", e);
            log.info("init JobMetrics context type:{}", context.getClass().getName());
            throw e;
        }
        this.inputDataNum = this.paMetrics.counter(nodeId + UserDefinedMetrics.INPUT_DATA_NUM_SUFFIX.getMetricName());
        this.outputDataNum = this.paMetrics.counter(nodeId + UserDefinedMetrics.OUTPUT_DATA_NUM_SUFFIX.getMetricName());
        this.dirtyDataNum = this.paMetrics.counter(nodeId + UserDefinedMetrics.DIRTY_DATA_NUM_SUFFIX.getMetricName());
        this.translateDelay = this.paMetrics.counter(nodeId + UserDefinedMetrics.TRANS_DELAY_SUFFIX.getMetricName());
        this.processDelay = this.paMetrics.counter(nodeId + UserDefinedMetrics.PROCESS_DELAY_SUFFIX.getMetricName());
        this.meter60 = this.paMetrics.meter(nodeId + UserDefinedMetrics.ONE_MINUTE_SUFFIX.getMetricName(), new MeterView(this.inputDataNum, 60));
        this.meter600 = this.paMetrics.meter(nodeId + UserDefinedMetrics.TEN_MINUTE_SUFFIX.getMetricName(), new MeterView(this.inputDataNum, 600));
        this.meter3600 = this.paMetrics.meter(nodeId + UserDefinedMetrics.ONE_HOUR_SUFFIX.getMetricName(), new MeterView(this.inputDataNum, 3600));
        this.meter86400 = this.paMetrics.meter(nodeId + UserDefinedMetrics.ONE_DAY_SUFFIX.getMetricName(), new MeterView(this.inputDataNum, 86400));
    }

    public MetricGroup getPaMetrics() {
        return this.paMetrics;
    }

    public String getNodeId() {
        return this.nodeId;
    }

    public Counter getInputDataNum() {
        return this.inputDataNum;
    }

    public Counter getProcessDelay() {
        return this.processDelay;
    }

    public Counter getTranslateDelay() {
        return this.translateDelay;
    }

    public Meter getMeter60() {
        return this.meter60;
    }

    public Meter getMeter600() {
        return this.meter600;
    }

    public Meter getMeter3600() {
        return this.meter3600;
    }

    public Meter getMeter86400() {
        return this.meter86400;
    }

    public Counter getOutputDataNum() {
        return this.outputDataNum;
    }

    public Counter getDirtyDataNum() {
        return this.dirtyDataNum;
    }

    public void setPaMetrics(final MetricGroup paMetrics) {
        this.paMetrics = paMetrics;
    }

    public void setNodeId(final String nodeId) {
        this.nodeId = nodeId;
    }

    public void setInputDataNum(final Counter inputDataNum) {
        this.inputDataNum = inputDataNum;
    }

    public void setProcessDelay(final Counter processDelay) {
        this.processDelay = processDelay;
    }

    public void setTranslateDelay(final Counter translateDelay) {
        this.translateDelay = translateDelay;
    }

    public void setMeter60(final Meter meter60) {
        this.meter60 = meter60;
    }

    public void setMeter600(final Meter meter600) {
        this.meter600 = meter600;
    }

    public void setMeter3600(final Meter meter3600) {
        this.meter3600 = meter3600;
    }

    public void setMeter86400(final Meter meter86400) {
        this.meter86400 = meter86400;
    }

    public void setOutputDataNum(final Counter outputDataNum) {
        this.outputDataNum = outputDataNum;
    }

    public void setDirtyDataNum(final Counter dirtyDataNum) {
        this.dirtyDataNum = dirtyDataNum;
    }

    public boolean equals(final Object o) {
        if (o == this) {
            return true;
        } else if (!(o instanceof JobMetrics)) {
            return false;
        } else {
            JobMetrics other = (JobMetrics) o;
            if (!other.canEqual(this)) {
                return false;
            } else {
                Object this$nodeId = this.getNodeId();
                Object other$nodeId = other.getNodeId();
                if (this$nodeId == null) {
                    if (other$nodeId != null) {
                        return false;
                    }
                } else if (!this$nodeId.equals(other$nodeId)) {
                    return false;
                }

                return true;
            }
        }
    }

    protected boolean canEqual(final Object other) {
        return other instanceof JobMetrics;
    }

    public int hashCode() {
        boolean PRIME = true;
        int result = 1;
        Object $nodeId = this.getNodeId();
        result = result * 59 + ($nodeId == null ? 43 : $nodeId.hashCode());
        return result;
    }

    public String toString() {
        return "JobMetrics(paMetrics=" + this.getPaMetrics() + ", nodeId=" + this.getNodeId() + ", inputDataNum=" + this.getInputDataNum() + ", processDelay=" + this.getProcessDelay() + ", translateDelay=" + this.getTranslateDelay() + ", meter60=" + this.getMeter60() + ", meter600=" + this.getMeter600() + ", meter3600=" + this.getMeter3600() + ", meter86400=" + this.getMeter86400() + ", outputDataNum=" + this.getOutputDataNum() + ", dirtyDataNum=" + this.getDirtyDataNum() + ")";
    }

    public JobMetrics(final MetricGroup paMetrics, final String nodeId, final Counter inputDataNum, final Counter processDelay, final Counter translateDelay, final Meter meter60, final Meter meter600, final Meter meter3600, final Meter meter86400, final Counter outputDataNum, final Counter dirtyDataNum) {
        this.paMetrics = paMetrics;
        this.nodeId = nodeId;
        this.inputDataNum = inputDataNum;
        this.processDelay = processDelay;
        this.translateDelay = translateDelay;
        this.meter60 = meter60;
        this.meter600 = meter600;
        this.meter3600 = meter3600;
        this.meter86400 = meter86400;
        this.outputDataNum = outputDataNum;
        this.dirtyDataNum = dirtyDataNum;
    }

    public JobMetrics() {
    }
}
*/
