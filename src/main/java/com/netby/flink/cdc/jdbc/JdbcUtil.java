package com.netby.flink.cdc.jdbc;

import com.zaxxer.hikari.HikariDataSource;

import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcUtil {
    private static final Logger LOG = LoggerFactory.getLogger(JdbcUtil.class);

    private static final String TABLE_PLACEHOLDER = "#table#";

    private static final String FROM_STR = " from ";

    static List<String> getAllTables(HikariDataSource dataSource, String tableNamePattern) throws SQLException {
        List<String> tables = new ArrayList<>();
        Connection connection = dataSource.getConnection();
        DatabaseMetaData metaData = connection.getMetaData();
        String catalog = connection.getCatalog();
        String schemaPattern = connection.getSchema();
        ResultSet result = metaData.getTables(catalog, schemaPattern, null, new String[]{"TABLE"});
        while (result.next()) {
            String tableName = result.getString(3);
            if (tableName.matches(tableNamePattern)) {
                tables.add(tableName);
                LOG.info("table [{}] match the table pattern [{}]", tableName, tableNamePattern);
            }
        }
        connection.close();
        return tables;
    }

    public static String getTablePatternFromSql(String sqlStr) {
        String sqlStrLow = sqlStr.toLowerCase();
        int fromIndex = sqlStrLow.lastIndexOf(" from ");
        if (fromIndex < 0) {
            throw new IllegalArgumentException("the select sql string is not correct,table could not find");
        }
        String fromStr = sqlStr.substring(fromIndex);
        String[] items = fromStr.split(" ");
        List<String> keywordList = new ArrayList<>();
        for (int i = 0; i < items.length; i++) {
            if (items[i].trim().length() > 0) {
                keywordList.add(items[i]);
            }
        }
        if (keywordList.size() > 1) {
            return keywordList.get(1);
        }
        LOG.warn("could not find table or table pattern form the given sql string");
        return "";
    }

    private static String tableNameReplaceHolder(String sqlStr) {
        String tableName = "";
        String[] strs = sqlStr.split(" ");
        for (String str : strs) {
            if (str.matches("[\\s\\S]*\\$\\{(.*)\\}[\\s\\S]*")) {
                tableName = str.trim();
                break;
            }
        }
        if (StringUtils.isNotBlank(tableName)) {
            return sqlStr.replace(tableName, "#table#");
        }
        return "";
    }

    static Map<String, String> assemblySelectSql(Set<String> tables, String selectSentence) {
        Map<String, String> selectSentences = new HashMap<>(tables.size());
        String templateSql = tableNameReplaceHolder(selectSentence);

        for (String table : tables) {
            String selectSqlStr = templateSql.replace("#table#", table);
            selectSentences.put(table, selectSqlStr);
        }
        return selectSentences;
    }

    public static void setRecordToStatement(PreparedStatement upload, int[] typesArray, Row row) throws SQLException {
        if (typesArray != null && typesArray.length > 0 && typesArray.length != row.getArity()) {
            LOG.warn("Column SQL types array doesn't match arity of passed Row! Check the passed array...");
        }

        if (typesArray == null) {

            for (int index = 0; index < row.getArity(); index++) {
                LOG.warn("Unknown column type for column {}. Best effort approach to set its value: {}.",

                        Integer.valueOf(index + 1), row
                                .getField(index));
                upload.setObject(index + 1, row.getField(index));
            }
        } else {

            for (int i = 0; i < row.getArity(); i++) {
                setField(upload, typesArray[i], row.getField(i), i);
            }
        }
    }

    public static void setField(PreparedStatement upload, int type, Object field, int index) throws SQLException {
        if (field == null) {
            upload.setNull(index + 1, type);
        } else {

            try {
                switch (type) {
                    case 0:
                        upload.setNull(index + 1, type);
                        return;
                    case -7:
                    case 16:
                        upload.setBoolean(index + 1, ((Boolean) field).booleanValue());
                        return;
                    case -16:
                    case -15:
                    case -1:
                    case 1:
                    case 12:
                        upload.setString(index + 1, (String) field);
                        return;
                    case -6:
                        upload.setByte(index + 1, ((Byte) field).byteValue());
                        return;
                    case 5:
                        upload.setShort(index + 1, ((Short) field).shortValue());
                        return;
                    case 4:
                        upload.setInt(index + 1, ((Integer) field).intValue());
                        return;
                    case -5:
                        upload.setLong(index + 1, ((Long) field).longValue());
                        return;
                    case 7:
                        upload.setFloat(index + 1, ((Float) field).floatValue());
                        return;
                    case 6:
                    case 8:
                        upload.setDouble(index + 1, ((Double) field).doubleValue());
                        return;
                    case 2:
                    case 3:
                        upload.setBigDecimal(index + 1, (BigDecimal) field);
                        return;
                    case 91:
                        upload.setDate(index + 1, (Date) field);
                        return;
                    case 92:
                        upload.setTime(index + 1, (Time) field);
                        return;
                    case 93:
                        upload.setTimestamp(index + 1, (Timestamp) field);
                        return;
                    case -4:
                    case -3:
                    case -2:
                        upload.setBytes(index + 1, (byte[]) field);
                        return;
                }
                upload.setObject(index + 1, field);
                LOG.warn("Unmanaged sql type ({}) for column {}. Best effort approach to set its value: {}.", new Object[]{

                        Integer.valueOf(type),
                        Integer.valueOf(index + 1), field
                });

            } catch (ClassCastException e) {

                String errorMessage = String.format("%s, field index: %s, field value: %s.", new Object[]{

                        e.getMessage(), Integer.valueOf(index), field});
                ClassCastException enrichedException = new ClassCastException(errorMessage);
                enrichedException.setStackTrace(e.getStackTrace());
                throw enrichedException;
            }
        }
    }
}