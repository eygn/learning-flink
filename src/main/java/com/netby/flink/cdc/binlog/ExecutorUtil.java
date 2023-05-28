package com.netby.flink.cdc.binlog;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author baiyg
 * @date 2023/1/12 10:59
 */
public class ExecutorUtil {

    private static ExecutorService nonCoreExecutorService = new ThreadPoolExecutor(1, 2, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingDeque<>(512));

    public static ExecutorService nonCore() {
        return nonCoreExecutorService;
    }
}
