package com.netby.flink.cdc.third;

import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

/**
 * @author: byg
 * @date: 2023/5/20 16:23
 */
@Data
public class DagNode implements Serializable {
    private static final long serialVersionUID = 2419337680766501301L;
    private boolean testMode;
    private String nodeId;
    private String jobId;
    private int nodeIndex;
    private int parallelism;
    private String className;
    private Properties properties;
    private NodeIOObject nodeIoObject;
    private transient List<DagNode> parentNode;
    private transient List<DagNode> childNode;
    private FilterNodeArgs2 filterNodeArgs;
    private DagNodeType nodeType;
    private boolean generatedSource;
    private String nodeName;
    private boolean printLog;
    private String fileName;

    public void addParentNode(DagNode jobNode) {
        if (jobNode != null) {
            if (this.parentNode == null) {
                this.parentNode = new ArrayList();
            }

            this.parentNode.add(jobNode);
        }
    }

    public void addChildNode(DagNode jobNode) {
        if (jobNode != null) {
            if (this.childNode == null) {
                this.childNode = new ArrayList();
            }

            this.childNode.add(jobNode);
        }
    }

    public List<Parameter> input() {
        List<Parameter> result = new ArrayList();
        if (this.nodeIoObject != null && this.nodeIoObject.getComponentInput() != null && !this.nodeIoObject.getComponentInput().isEmpty()) {
            Iterator var2 = this.nodeIoObject.getComponentInput().iterator();

            while(var2.hasNext()) {
                String str = (String)var2.next();
                Iterator var4 = this.nodeIoObject.getInputObjects().iterator();

                while(var4.hasNext()) {
                    InputObject io = (InputObject)var4.next();
                    if (io.getName().equals(str)) {
                        result.add(new Parameter(str, SupportType.valueOf(io.getType().toUpperCase())));
                    }
                }
            }

            return result;
        } else {
            return result;
        }
    }

    public List<Parameter> output() {
        List<Parameter> result = new ArrayList();
        if (this.nodeIoObject != null && this.nodeIoObject.getComponentOutput() != null && !this.nodeIoObject.getComponentOutput().isEmpty()) {
            Iterator var2 = this.nodeIoObject.getComponentOutput().iterator();

            while(var2.hasNext()) {
                String str = (String)var2.next();
                Iterator var4 = this.nodeIoObject.getOutputObjects().iterator();

                while(var4.hasNext()) {
                    OutputObject oo = (OutputObject)var4.next();
                    if (oo.getName().equals(str)) {
                        result.add(new Parameter(str, SupportType.valueOf(oo.getType().toUpperCase())));
                    }
                }
            }

            return result;
        } else {
            return result;
        }
    }

    public List<Parameter> getNodeInput() {
        List<Parameter> result = new ArrayList();
        if (this.nodeIoObject != null && this.nodeIoObject.getInputObjects() != null && !this.nodeIoObject.getInputObjects().isEmpty()) {
            Iterator var2 = this.nodeIoObject.getInputObjects().iterator();

            while(var2.hasNext()) {
                InputObject io = (InputObject)var2.next();
                result.add(new Parameter(io.getName(), SupportType.valueOf(io.getType().toUpperCase())));
            }

            return result;
        } else {
            return result;
        }
    }

    public List<Parameter> getNodeOutput() {
        List<Parameter> result = new ArrayList();
        if (this.nodeIoObject != null && this.nodeIoObject.getOutputObjects() != null && !this.nodeIoObject.getOutputObjects().isEmpty()) {
            Iterator var2 = this.nodeIoObject.getOutputObjects().iterator();

            while(var2.hasNext()) {
                OutputObject oo = (OutputObject)var2.next();
                if (oo.isOutputable()) {
                    result.add(new Parameter(oo.getName(), SupportType.valueOf(oo.getType().toUpperCase())));
                }
            }

            return result;
        } else {
            return result;
        }
    }

    public List<String> getNodeInputNames() {
        List<String> result = new ArrayList();
        if (this.nodeIoObject != null && this.nodeIoObject.getInputObjects() != null && !this.nodeIoObject.getInputObjects().isEmpty()) {
            Iterator var2 = this.nodeIoObject.getInputObjects().iterator();

            while(var2.hasNext()) {
                InputObject io = (InputObject)var2.next();
                result.add(io.getName());
            }

            return result;
        } else {
            return result;
        }
    }

    public List<String> getNodeOutputNames() {
        List<String> result = new ArrayList();
        if (this.nodeIoObject != null && this.nodeIoObject.getOutputObjects() != null && !this.nodeIoObject.getOutputObjects().isEmpty()) {
            Iterator var2 = this.nodeIoObject.getOutputObjects().iterator();

            while(var2.hasNext()) {
                OutputObject oo = (OutputObject)var2.next();
                if (oo.isOutputable()) {
                    result.add(oo.getName());
                }
            }

            return result;
        } else {
            return result;
        }
    }

}
