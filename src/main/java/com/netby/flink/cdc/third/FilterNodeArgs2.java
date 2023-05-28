package com.netby.flink.cdc.third;


import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import lombok.Data;
import org.apache.commons.collections.MapUtils;

/**
 * @author: byg
 * @date: 2023/5/20 16:25
 */
@Data
public class FilterNodeArgs2 implements Serializable {
    private static final long serialVersionUID = -6405187982781747744L;
    private List<String> trueNodeId = new ArrayList();
    private List<String> falseNodeId = new ArrayList();
    private FilterNode2 filterNode2;
    private static final String POINT_OUT_TYPE = "type";
    private static final String POINT_OUT_UUID = "uuid";
    private static final String TRUE = "true";
    private static final String FALSE = "false";
    private static final String POINT_OUT = "pointOut";
    private static final String PROPERTIES = "properties";

    public FilterNodeArgs2() {
    }

    public static FilterNodeArgs2 getInstance(JSONArray pointOut, JSONObject filterNodeProperties) {
        FilterNodeArgs2 filterNodeArgs = new FilterNodeArgs2();
        if (pointOut != null && !pointOut.isEmpty()) {
            Iterator var3 = pointOut.iterator();

            while(var3.hasNext()) {
                Object obj = var3.next();
                if (obj instanceof JSONObject) {
                    JSONObject json = (JSONObject)obj;
                    if ("true".equals(json.getString("type"))) {
                        filterNodeArgs.trueNodeId.add(json.getString("uuid"));
                    } else if ("false".equals(json.getString("type"))) {
                        filterNodeArgs.falseNodeId.add(json.getString("uuid"));
                    }
                }
            }

            if (!MapUtils.isEmpty(filterNodeProperties)) {
                filterNodeArgs.filterNode2 = (FilterNode2)JSONObject.parseObject(filterNodeProperties.toJSONString(), FilterNode2.class);
            }

            return filterNodeArgs;
        } else {
            return null;
        }
    }

}
