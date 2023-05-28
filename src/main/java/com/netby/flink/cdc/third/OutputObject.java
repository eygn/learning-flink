package com.netby.flink.cdc.third;


import lombok.Data;

import java.io.Serializable;
import java.util.List;

/**
 * @author: byg
 * @date: 2023/5/20 16:55
 */
@Data
public class OutputObject implements Serializable {
    private static final long serialVersionUID = -8308918109921610627L;
    private String name;
    private String type;
    private String method;
    private List<ParamsObject> params;
    private boolean outputable;
    private boolean targetable;
    private boolean disabled;

}
