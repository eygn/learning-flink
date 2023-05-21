package com.netby.lerning.flink.cdc.third;

/**
 * @author: byg
 * @date: 2023/5/20 16:27
 */
public enum FuncType {
    DEFAULT("DEFAULT"),
    PRIVATE("PRIVATE"),
    SHARE("SHARE");

    private final String name;

    private FuncType(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }
}

