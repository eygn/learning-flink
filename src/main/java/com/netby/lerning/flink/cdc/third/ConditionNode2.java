package com.netby.lerning.flink.cdc.third;



import lombok.Data;

import java.io.Serializable;
import java.util.List;

/**
 * @author: byg
 * @date: 2023/5/20 16:27
 */
@Data
public class ConditionNode2 implements Serializable {
    private static final long serialVersionUID = -2365934231935781232L;
    private String name;
    private String funcName;
    private FuncType funcType;
    private List<FuncArgs> funcArgs;
}
