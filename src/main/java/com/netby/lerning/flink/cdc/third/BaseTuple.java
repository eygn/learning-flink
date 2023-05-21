package com.netby.lerning.flink.cdc.third;

import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;

/**
 * @author: byg
 * @date: 2023/5/20 16:30
 */

public abstract class BaseTuple implements Serializable {
    public static final int MAX_ARITY = 200;
    private static final long serialVersionUID = 3881773983392251749L;
    private String uuid;
    private long emitTime;
    private String routePath;

    public BaseTuple() {
    }

    public String getRoutePath() {
        return this.routePath;
    }

    public void setRoutePath(String routePath) {
        this.routePath = routePath;
    }

    public String getUuid() {
        return this.uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public abstract <T> T getField(int pos);

    public abstract <T> void setField(int pos, T value);

    public abstract int size();

    public abstract <T extends BaseTuple> T copy();

    public static BaseTuple newInstance(int size) {
        switch (size) {
            default:
                return new TupleN(size);
        }
    }

    public long getEmitTime() {
        return this.emitTime;
    }

    public void setEmitTime(long emitTime) {
        this.emitTime = emitTime;
    }

    public boolean isNotEmpty(BaseTuple tuple) {
        for (int i = 0; i < tuple.size(); ++i) {
            Object fieldValue = tuple.getField(i);
            if (fieldValue != null && StringUtils.isNotBlank(fieldValue.toString())) {
                return true;
            }
        }

        return false;
    }
}
