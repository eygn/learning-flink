package com.netby.lerning.flink.cdc.third;

/**
 * @author: byg
 * @date: 2023/5/20 16:50
 */
public enum DagNodeType {
    SOURCE,
    SINK,
    PROCESS,
    FILTER,
    MERGE,
    FLAT_MAP,
    JOIN,
    SORT,
    GLOBAL_AGG,
    Aggregate,
    ROUTETEMPLATE,
    DYNAMIC_FILTER,
    DATA_QUALITY_MONITOR;

    private DagNodeType() {
    }

    public static DagNodeType dagNodeType(ClassLoader classLoader, String componentClassName) throws ClassNotFoundException {
        try {
            Class<?> cla = classLoader.loadClass(componentClassName);
            return SOURCE;
        } catch (ClassNotFoundException var3) {
            throw new ClassNotFoundException("component not exists: " + componentClassName);
        }
    }
}
