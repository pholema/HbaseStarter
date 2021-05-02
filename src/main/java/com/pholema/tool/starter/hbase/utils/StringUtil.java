package com.pholema.tool.starter.hbase.utils;

import java.util.Arrays;
import java.util.List;

public class StringUtil {
    public static String ReplaceColon(String source){
    	return source.replace(":", "_");
    }
    
    public static String SQLType2JavaTypeInString(String source_type){
    	String java_type=null;
		if (source_type.startsWith("money")
				||source_type.startsWith("decimal")
				||source_type.startsWith("smallmoney")
		){
			java_type="double";
		}else if (source_type.startsWith("numeric")){
			java_type="bigdecimal";
		}else if (source_type.startsWith("int")
				||source_type.startsWith("smallint")
				||source_type.startsWith("tinyint")
				||source_type.startsWith("bit")){
			java_type="integer";
		}else if (source_type.startsWith("bigint")){
			java_type="long";
		}else if (source_type.startsWith("real")||
				source_type.startsWith("float")){
			java_type="float";
		}else if (source_type.startsWith("varchar")
				||source_type.startsWith("longvarchar")
				||source_type.startsWith("character")
				||source_type.startsWith("nvarchar")
				||source_type.startsWith("char")
				||source_type.startsWith("nchar")
				||source_type.startsWith("text")
				||source_type.startsWith("ntext")
				||source_type.startsWith("uniqueidentifier")
				||source_type.startsWith("sysname")
				){
			java_type="string";
		}else if (source_type.startsWith("date")
				||source_type.startsWith("datetime")
				||source_type.startsWith("datetime2")
				||source_type.startsWith("time")
				||source_type.startsWith("smalldatetime")){
			java_type="date";
		}
		return java_type;
    }
    
    

	public static String GetRowKey(String HBaseKeyPrefix,String rowKeyValues){
		return GetRowKey(HBaseKeyPrefix,Arrays.asList((rowKeyValues).split("\\s*,\\s*")));
	}
	
	public static String GetRowKey(String HBaseKeyPrefix,List<String> rowKeyValues){
		String rowKey=HBaseKeyPrefix;
		for(String key: rowKeyValues)
			rowKey=((rowKey.equals(""))?"":rowKey.concat("-")).concat(key);
		return rowKey;
	}
	
	public static String nullHandle(Object o) {
		return o==null?null:o.toString();
	}
	
}
