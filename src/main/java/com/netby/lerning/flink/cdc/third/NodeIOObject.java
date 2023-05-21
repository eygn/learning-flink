package com.netby.lerning.flink.cdc.third;


import com.sun.corba.se.pept.encoding.OutputObject;
import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * @author: byg
 * @date: 2023/5/20 16:48
 */
@Data
public class NodeIOObject implements Serializable {
    private static final long serialVersionUID = -3968116291036834943L;
    private List<InputObject> inputObjects = new ArrayList();
    private List<OutputObject> outputObjects = new ArrayList();
    private List<String> componentInput = new ArrayList();
    private List<String> componentOutput = new ArrayList();
}
