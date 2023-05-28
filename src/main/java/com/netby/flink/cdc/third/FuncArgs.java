package com.netby.flink.cdc.third;

/**
 * @author: byg
 * @date: 2023/5/20 16:28
 */

import lombok.Builder;
import lombok.Data;
import lombok.experimental.Accessors;

import java.io.Serializable;

@Data
@Builder
@Accessors(chain = true)
public class FuncArgs implements Serializable {

    private static final long serialVersionUID = -2001055957982616816L;

    private String name;
    private String type;
    private String key;
    private String value;

}
