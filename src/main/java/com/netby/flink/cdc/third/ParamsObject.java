package com.netby.flink.cdc.third;

/**
 * @author: byg
 * @date: 2023/5/20 16:55
 */

import lombok.Data;

import java.io.Serializable;

@Data
public class ParamsObject implements Serializable {
    private static final long serialVersionUID = 3989690479609025485L;
    private String type;
    private String value;
    public static final String PREFIX_OF_PARAMETER = "$";

}
