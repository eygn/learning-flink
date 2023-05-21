package com.netby.lerning.flink.cdc.third;


import lombok.Data;

import java.io.Serializable;

/**
 * @author: byg
 * @date: 2023/5/20 16:49
 */
@Data
public class InputObject implements Serializable {
    private static final long serialVersionUID = 2857510260901348871L;
    private String name;
    private String type;
    private boolean targetable;

}
