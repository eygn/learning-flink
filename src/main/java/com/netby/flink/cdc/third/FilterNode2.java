package com.netby.flink.cdc.third;



import lombok.Data;

import java.io.Serializable;
import java.util.List;

/**
 * @author: byg
 * @date: 2023/5/20 16:26
 */
@Data
public class FilterNode2 implements Serializable {
    private static final long serialVersionUID = -3925096215828750150L;
    private String logicType;
    private String expr;
    List<ConditionNode2> conditions;

}