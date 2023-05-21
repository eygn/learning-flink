package com.netby.lerning.flink.cdc.third;

import java.nio.charset.StandardCharsets;
import java.util.Date;

/**
 * @author: byg
 * @date: 2023/5/20 16:23
 */
public enum SupportType {
    INTEGER(Integer.class.getSimpleName()),
    LONG(Long.class.getSimpleName()),
    DOUBLE(Double.class.getSimpleName()),
    FLOAT(Float.class.getSimpleName()),
    BOOLEAN(Boolean.class.getSimpleName()),
    STRING(String.class.getSimpleName()),
    BYTE(Byte.class.getSimpleName()),
    BYTES("bytes"),
    DATE(Date.class.getSimpleName()),
    OBJECT("object");

    private String name;

    private SupportType(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }

    public static Class<?> typeClass(SupportType type) {
        switch (type) {
            case STRING:
                return String.class;
            case INTEGER:
                return Integer.TYPE;
            case LONG:
                return Long.TYPE;
            case DATE:
                return Date.class;
            case BYTE:
                return Byte.TYPE;
            case FLOAT:
                return Float.TYPE;
            case BYTES:
                return byte[].class;
            case DOUBLE:
                return Double.TYPE;
            case BOOLEAN:
                return Boolean.TYPE;
            case OBJECT:
                return Object.class;
            default:
                return null;
        }
    }

    public static Object stringToExpectType(SupportType type, String str) {
        if (str == null) {
            return null;
        } else {
            switch (type) {
                case INTEGER:
                    return Integer.parseInt(str);
                case LONG:
                    return Long.parseLong(str);
                case DATE:
                    return new Date(Long.parseLong(str));
                case BYTE:
                default:
                    return str;
                case FLOAT:
                    return Float.parseFloat(str);
                case BYTES:
                    return str.getBytes(StandardCharsets.UTF_8);
                case DOUBLE:
                    return Double.parseDouble(str);
                case BOOLEAN:
                    return Boolean.parseBoolean(str);
            }
        }
    }
}
