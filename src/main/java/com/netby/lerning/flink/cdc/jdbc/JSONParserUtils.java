package com.netby.lerning.flink.cdc.jdbc;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

/**
 * @author baiyg
 * @date 2022/12/24 17:08
 */


public class JSONParserUtils {

    public static Map<String, Object> jsonToMap(String jsonStr, String keyPrefix) {

        if (keyPrefix == null) {
            keyPrefix = "";
        }

        Map<String, Object> keyValueMap = new TreeMap<String, Object>();

        /*
         * 当值为字符串数组（如："pages_en":["one","two","three"]），递归解析字符串时，会抛异常（如：JSON.parse("one") ）。
         * 这里进行异常捕获，按键值对进行储存。
         * 详情查看 src/main/java/org/kwok/util/json/Test_OrgJson_Parse.java 。
         */
        Object obj;
        try {
            obj = JSONObject.parseObject(jsonStr);
        } catch (Exception e1) {
            try {
                obj = JSONArray.parseArray(jsonStr);
            } catch (Exception e2) {
                obj = jsonStr;
            }
        }

        if (obj instanceof JSONObject) {
            JSONObject jsonObject = JSONObject.parseObject(jsonStr);
            Iterator<String> keys = jsonObject.keySet().iterator();
            while (keys.hasNext()) {
                String key = keys.next();
                if (jsonObject.get(key) instanceof JSONObject) {
                    String tempKeyPrefix = keyPrefix + key + ".";
                    keyValueMap.putAll(jsonToMap(jsonObject.get(key).toString(), tempKeyPrefix));
                } else if (jsonObject.get(key) instanceof JSONArray) {
                    String tempKeyPrefix = keyPrefix + key + ".";
                    keyValueMap.putAll(jsonToMap(jsonObject.get(key).toString(), tempKeyPrefix));
                } else {
                    /*
                     * 处理属性值为 null 的情况，这里转为空字符串。
                     */
                    if (jsonObject.get(key) == null) {
                        keyValueMap.put(keyPrefix + key, "");
                    } else {
                        keyValueMap.put(keyPrefix + key, jsonObject.get(key));
                    }
                }
            }
        } else if (obj instanceof JSONArray) {
            JSONArray jsonArray = JSONArray.parseArray(jsonStr);
            for (int i = 0; i < jsonArray.size(); i++) {
                String tempKeyPrefix = keyPrefix == "" ? keyPrefix + "[" + i + "]" + "." : keyPrefix.substring(0, keyPrefix.length() - 1) + "[" + i + "]" + ".";
                /*
                 * 处理数组中元素为 null 的情况，这里转为空字符串。如：{"pages":[1,2,null]}。
                 */
                if (jsonArray.get(i) == null) {
                    keyValueMap.putAll(jsonToMap("", tempKeyPrefix));
                } else {
                    keyValueMap.putAll(jsonToMap(jsonArray.get(i).toString(), tempKeyPrefix));
                }

            }
        } else {
            /*
             * 当值为数组，递归时进入该分支。如：{"pages":[1,2,3]}。
             */
            keyValueMap.put(keyPrefix == "" ? keyPrefix : keyPrefix.substring(0, keyPrefix.length() - 1), jsonStr);
        }
        return keyValueMap;
    }


    /**
     * JSON TO Properties
     * 注：由于  Properties 继承自 Hashtable，固 key、value 值均不可为 null。
     * 详情查看 src/main/java/org/kwok/util/json/Test_Properties.java。
     *
     * @author Kwok
     **/
    public static Properties jsonToProperties(String jsonStr) {
        Properties properties = new Properties();
        properties.putAll(jsonToMap(jsonStr, null));
        return properties;
    }

}