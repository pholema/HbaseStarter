package com.pholema.tool.starter.hbase.utils;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

public class HiveUtils {

    public static void main(String[] args) {
        String hiveTableName = System.getProperty("hiveTableName");
        String hbaseTableName = System.getProperty("hbaseTableName");
        String columnFamily = System.getProperty("columnFamily");
        String mClassName = System.getProperty("mClassName");
        System.out.println(generateHiveScript(hiveTableName, hbaseTableName, columnFamily, mClassName));
    }


    public static String generateHiveScript(String hiveTableName, String hbaseTableName, String columnFamily, String mClassName) {
        Map<String,String> columns = new HashMap<>();
        Class<?> mClass;
        try {
            mClass = Class.forName(mClassName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return null;
        }

        Method[] method = mClass.getMethods(); // include all methods

        for (int i = 0; i < method.length; i++) {
            String methodName = method[i].getName();
            if (methodName.startsWith("get")) {
                String columnName = method[i].getName().replace("get", "");
                if (columnName.toLowerCase().equals("class"))
                    continue; // ignore class name

                String type;
                System.out.println("name " + method[i].getReturnType().getName());
                switch (method[i].getReturnType().getName()) {
                    case "java.lang.Long":
                        type = "Bigint";
                        break;
                    case "java.lang.Integer":
                    case "int":
                        type = "Int";
                        break;
                    case "java.lang.Boolean":
                        columnName = "Is".concat(columnName.substring(0, 1).toUpperCase() + columnName.substring(1));
                        type = "Boolean";
                        break;
                    case "java.util.Date":
                        type = "String";
                        break;
                    default:
                        String[] strs = method[i].getReturnType().getName().split("\\.");
                        type = strs[strs.length - 1];
                        break;
                }
                columns.put(columnName, type);
            } else if (methodName.startsWith("is")) {
                // type = boolean
                System.out.println("name " + method[i].getReturnType().getName());
                String columnName = method[i].getName();
                columnName = columnName.substring(0, 1).toUpperCase() + columnName.substring(1);
                columns.put(columnName, "Boolean");
            }
        }

        StringBuilder builder = new StringBuilder();
        builder.append("CREATE EXTERNAL TABLE IF NOT EXISTS "+hiveTableName+"(");
        builder.append("key String");
        int line = 0;
        for (Map.Entry entry : columns.entrySet()) {
            line++;
            builder.append(",");
            if (line == 4) {
                builder.append("\n");
                line = 0;
            }
            builder.append(entry.getKey() + " " + entry.getValue());
        }
        builder.append("\n) STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' WITH SERDEPROPERTIES ('hbase.columns.mapping' = '");
        builder.append(":key");
        line = 0;
        for (Map.Entry entry : columns.entrySet()) {
            line++;
            builder.append(",");
            if (line == 4) {
                builder.append("\n");
                line = 0;
            }
            builder.append(columnFamily + ":" + entry.getKey());
        }
        builder.append("') \nTBLPROPERTIES ('hbase.table.name' = '"+hbaseTableName+"');");
        return builder.toString();
    }

}
