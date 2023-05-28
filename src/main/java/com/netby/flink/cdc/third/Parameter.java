package com.netby.flink.cdc.third;

import lombok.Data;

import java.io.Serializable;

/**
 * @author: byg
 * @date: 2023/5/20 16:24
 */
@Data
public class Parameter implements Serializable {
    private static final long serialVersionUID = -3637317045007116252L;
    private int position;
    private String name;
    private SupportType type;

    public Parameter() {
    }

    public Parameter(String name, SupportType type) {
        this.name = name;
        this.type = type;
    }

    public Parameter(int position, String name, SupportType type) {
        this.position = position;
        this.name = name;
        this.type = type;
    }
}

