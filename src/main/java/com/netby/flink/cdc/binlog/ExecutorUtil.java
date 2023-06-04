package com.netby.flink.cdc.binlog;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.*;

/**
 * @author baiyg
 * @date 2023/1/12 10:59
 */
@Slf4j
public class ExecutorUtil {

    private static ExecutorService nonCoreExecutorService =
            new ThreadPoolExecutor(1, 2, 60L, TimeUnit.MILLISECONDS, new LinkedBlockingDeque<>(512), new RejectedExecutionHandler() {
                @Override
                public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                    log.error("非核心线程池任务丢失");
                }
            });

    /**
     * 非核心线程池
     *
     * @return
     */
    public static ExecutorService nonCore() {
        return nonCoreExecutorService;
    }
}
