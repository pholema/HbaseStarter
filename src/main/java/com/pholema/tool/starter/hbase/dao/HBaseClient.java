package com.pholema.tool.starter.hbase.dao;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import com.pholema.tool.starter.hbase.utils.DateUtil;
import com.pholema.tool.starter.hbase.utils.StringUtil;

public class HBaseClient {
	private static final Logger logger = LoggerFactory.getLogger(HBaseClient.class);
	private org.apache.hadoop.conf.Configuration HBaseConf = org.apache.hadoop.hbase.HBaseConfiguration.create();
	private Connection HBaseConnection;
	private static HBaseClient instance = null;
	private static int TIMEOUT = 120000;
	private static String MASTER = "127.0.0.1:60000*";
	private static String ZOOKEEPER_QUORUM = "127.0.0.1,127.0.0.2";
	private static String ZOOKEEPER_CLIENTPORT = "2181";
	private static Gson gson = new Gson();

	public void init() {
		HBaseConf.clear();
		HBaseConf.setInt("timeout", TIMEOUT);
		HBaseConf.set("hbase.master", MASTER);
		HBaseConf.set("hbase.zookeeper.quorum", ZOOKEEPER_QUORUM);
		HBaseConf.set("hbase.zookeeper.property.clientPort", ZOOKEEPER_CLIENTPORT);
		HBaseConf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		HBaseConf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
		try {
			HBaseConnection = ConnectionFactory.createConnection(HBaseConf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void init(String name) {
		HBaseConf.clear();
		HBaseConf.addResource(name);
		try {
			HBaseConnection = ConnectionFactory.createConnection(HBaseConf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * connect Hbase server and return HBaseDAO instance
	 * 
	 * @return
	 */
	public static HBaseClient getInstance() {
		if (instance == null) {
			instance = new HBaseClient();
		}
		return instance;
	}

	/**
	 * connect Hbase server and return HBaseDAO instance
	 * 
	 * @param timeout
	 * @param master
	 * @param zookeeperQuorum
	 * @param zookeeperClientPort
	 * @return
	 */
	public static HBaseClient getInstance(int timeout, String master, String zookeeperQuorum,
			String zookeeperClientPort) {
		TIMEOUT = timeout;
		MASTER = master;
		ZOOKEEPER_QUORUM = zookeeperQuorum;
		ZOOKEEPER_CLIENTPORT = zookeeperClientPort;
		if (instance == null) {
			instance = new HBaseClient();
		}
		return instance;
	}

	/**
	 * Post single specific column & string value to Hbase by BufferedMutator
	 * 
	 * @param tableName
	 * @param columnFamily
	 * @param columnName
	 * @param map
	 * @return
	 * @throws Exception
	 */
	public boolean postStringValueBySingleColumn(String tableName, String columnFamily, String columnName,
			Map<String, String> map) throws Exception {
		try {
			BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf(tableName));
			params.writeBufferSize(4 * 1024 * 1024);
			BufferedMutator mutator = HBaseConnection.getBufferedMutator(params);

			// Table
			// table=HBaseConnection.getTable(TableName.valueOf(tableName));
			Integer total = 0;

			for (String rowKey : map.keySet()) {
				Put put = treatHBaseOneStringColumn(columnFamily, columnName, rowKey, map.get(rowKey));
				mutator.mutate(put);
				total += 1;
			}
			mutator.flush();
			logger.info(tableName + "[" + columnFamily + "] Total Processed: " + total);
			return true;
		} catch (IllegalArgumentException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}

	/**
	 * Post multiple specific column & string value to Hbase by BufferedMutator
	 * 
	 * @param tableName
	 * @param columnFamily
	 * @param map          : ＜rowkey,＜columnName,value＞＞
	 * @return
	 * @throws Exception
	 */
	public boolean postStringValueByMultiColumn(String tableName, String columnFamily,
			Map<String, Map<String, String>> map) throws Exception {
		try {
			BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf(tableName));
			params.writeBufferSize(4 * 1024 * 1024);
			BufferedMutator mutator = HBaseConnection.getBufferedMutator(params);
			Integer total = 0;

			for (String rowKey : map.keySet()) {
				Map<String, String> sub_map = map.get(rowKey);
				for (String columnName : sub_map.keySet()) {
					Put put = treatHBaseOneStringColumn(columnFamily, columnName, rowKey, sub_map.get(columnName));
					mutator.mutate(put);
					total += 1;
				}
			}
			mutator.flush();
			logger.info(tableName + "[" + columnFamily + "] Total Processed: " + total);
			return true;
		} catch (IllegalArgumentException | IOException e) {
			e.printStackTrace();
			return false;
		}
	}

	private <M> Put treatHBaseOneStringColumn(String columnFamily, String columnName, String rowKey, String value) {
		Put put = new Put(Bytes.toBytes(rowKey));
		try {
			put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName), Bytes.toBytes(value));
		} catch (Exception e) {
			StringWriter sw = new StringWriter();
			e.printStackTrace(new PrintWriter(sw));
			logger.error("TreatHBaseString error" + sw.toString());
		}
		return put;
	}

	private Table GetHBaseTable(String tableName) throws IOException {
		return HBaseConnection.getTable(TableName.valueOf(tableName));
	}

	/**
	 * Post the entry object to Hbase by BufferedMutator
	 * 
	 * @param tableName
	 * @param columnFamily
	 * @param mClass
	 * @param map
	 * @return
	 * @throws Exception
	 */
	public <M> boolean postEntryMutator(String tableName, String columnFamily, Class<M> mClass, Map<String, Object> map)
			throws Exception {
		return postEntryMutator(tableName, columnFamily, mClass, map, null);
	}

	/**
	 * Post the entry object to Hbase by BufferedMutator if subRowKeyName !=null
	 * then key column(keyName+keyValue) value soNumber1 itemnumber-00-995-291
	 * jsonString itemnumber-00-995-292 jsonString itemnumber-00-995-293 jsonString
	 * soNumber2 itemnumber-00-995-291 jsonString
	 *
	 * @param tableName
	 * @param columnFamily
	 * @param mClass
	 * @param map
	 * @param subRowKeyName
	 * @return
	 * @throws Exception
	 */
	private <M> boolean postEntryMutator(String tableName, String columnFamily, Class<M> mClass,
			Map<String, Object> map, List<String> subRowKeyNames) throws Exception {
		try {
			List<String> subRowKeyNames_lowerCase = new ArrayList<>();
			if (subRowKeyNames != null) {
				for (String subRowKeyName : subRowKeyNames) {
					subRowKeyNames_lowerCase.add(subRowKeyName.toLowerCase());
				}
			}

			// final ExecutorService workerPool =
			// Executors.newFixedThreadPool(20);
			// List<Future<Void>> futures = new ArrayList<>();

			BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf(tableName));
			params.writeBufferSize(4 * 1024 * 1024);
			BufferedMutator mutator = HBaseConnection.getBufferedMutator(params);

			// Table
			// table=HBaseConnection.getTable(TableName.valueOf(tableName));
			Integer total = 0;

			for (String rowKey : map.keySet()) {
				Put put = null;
				if (subRowKeyNames_lowerCase.size() == 0) {
					put = treatHBaseEntry(columnFamily, rowKey, mClass, map.get(rowKey));
				} else {
					// must testing
					put = treatHBaseEntry4Subkey(columnFamily, rowKey, subRowKeyNames_lowerCase, mClass,
							map.get(rowKey));
				}
				if (put != null) {
					mutator.mutate(put);
					total += 1;
				}
			}
			mutator.flush();
			logger.info(tableName + "[" + columnFamily + "] Total Processed: " + total);
			return true;
		} catch (IllegalArgumentException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}

	@SuppressWarnings("unchecked")
	public <M> Put treatHBaseEntry(String columnFamily, String rowKey, Class<M> mClass, Object obj) {
		Put put = new Put(Bytes.toBytes(rowKey));
		try {
			// Method [] method = mClass.getDeclaredMethods() ;
			Method[] method = mClass.getMethods(); // include all methods
													// extending another

			M entity = (M) obj;

			if (entity != null) {
				for (int i = 0; i < method.length; i++) {
					String methodName = method[i].getName();
					String columnName = null;
					String columnValue = null;
					if (methodName.startsWith("get")) {
						columnName = method[i].getName().replace("get", "");
						if (columnName.toLowerCase().equals("class"))
							continue; // ignore class name

						Object o = null;
						try {
							o = method[i].invoke(entity);
						} catch (Exception e) {
							e.printStackTrace();
						}
						switch (method[i].getReturnType().getName()) {
						case "java.util.Date":
							columnValue = o == null ? null : DateUtil.getDateTime((Date) o, "yyyy-MM-dd HH:mm:ss");
							break;
						case "java.lang.Boolean":
							columnName = "Is"
									.concat(columnName.substring(0, 1).toUpperCase() + columnName.substring(1));
						default:
							columnValue = StringUtil.nullHandle(o);
							break;
						}
					} else if (methodName.startsWith("is")) {
						// type = boolean
						Object o = null;
						try {
							o = method[i].invoke(entity);
						} catch (Exception e) {
							e.printStackTrace();
						}
						columnName = method[i].getName();
						columnName = columnName.substring(0, 1).toUpperCase() + columnName.substring(1);
						columnValue = StringUtil.nullHandle(o);
					}
					if (columnValue != null) {
						put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName),
								Bytes.toBytes(columnValue));
					}
				}
			}
		} catch (Exception e) {
			StringWriter sw = new StringWriter();
			e.printStackTrace(new PrintWriter(sw));
			logger.error("TreatHBaseEntry error" + sw.toString());
		}
		return put;
	}

	/**
	 * key column(keyName+keyValue) value soNumber1 itemnumber-00-995-291 jsonString
	 * itemnumber-00-995-292 jsonString itemnumber-00-995-293 jsonString soNumber2
	 * itemnumber-00-995-291 jsonString
	 */

	@SuppressWarnings("unchecked")
	public <M> Put treatHBaseEntry4Subkey(String columnFamily, String rowKey, List<String> subKeyNames, Class<M> mClass,
			Object obj) {
		Put put = new Put(Bytes.toBytes(rowKey));
		String subRowKey = "";
		try {
			// Method [] method = mClass.getDeclaredMethods() ;
			Method[] method = mClass.getMethods(); // include all methods
													// extending another

			M entity = (M) obj;

			if (entity != null) {

				for (int i = 0; i < method.length; i++) {
					String methodName = method[i].getName();
					if (methodName.startsWith("get")) {
						String columnName = method[i].getName().replace("get", "").toLowerCase();
						if (subKeyNames.contains(columnName)) {
							Object o = null;
							String columnValue = null;
							try {
								o = method[i].invoke(entity);
							} catch (Exception e) {
								e.printStackTrace();
							}
							columnValue = StringUtil.nullHandle(o);

							if (!subRowKey.equals(""))
								subRowKey += ";";
							subRowKey += columnName + "-" + columnValue.trim();

						}
					}
				}
				put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(subRowKey), Bytes.toBytes(gson.toJson(obj)));
			}
		} catch (Exception e) {
			StringWriter sw = new StringWriter();
			e.printStackTrace(new PrintWriter(sw));
			logger.error("treatHBaseEntry4Subkey error" + sw.toString());
		}
		return put;
	}

	/**
	 * hbase get
	 * 
	 * @param tableName
	 * @param columnFamily
	 * @param rowKey
	 * @return
	 */
	public JsonObject get(String tableName, String columnFamily, String rowKey) {
		JsonObject innerObject = new JsonObject();
		byte[] TABLE_NAME = Bytes.toBytes(tableName);
		try {
			Table table = HBaseConnection.getTable(TableName.valueOf(TABLE_NAME));

			Get g = new Get(Bytes.toBytes(rowKey));
			Result rs = table.get(g);
			if (rs == null)
				return (null);
			if (!rs.isEmpty()) {
				innerObject = get(rs, columnFamily);
			}
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return innerObject;
	}

	/**
	 * hbase get for rowKeys
	 * 
	 * @param tableName
	 * @param columnFamily
	 * @param rowKeys
	 * @return
	 */
	public List<JsonObject> get(String tableName, String columnFamily, List<String> rowKeys) {
		List<JsonObject> list = new ArrayList<>();
		JsonObject innerObject = new JsonObject();
		byte[] TABLE_NAME = Bytes.toBytes(tableName);
		try {
			Table table = HBaseConnection.getTable(TableName.valueOf(TABLE_NAME));

			List<Get> gets = new ArrayList<>();
			for (String rowKey : rowKeys) {
				Get g = new Get(Bytes.toBytes(rowKey));
				gets.add(g);
			}

			Result[] rs = table.get(gets);
			if (rs == null || rs.length == 0)
				return list;
			for (Result r : rs) {
				if (r == null)
					continue;
				innerObject = get(r, columnFamily);
				if (innerObject != null)
					list.add(innerObject);
			}
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return list;
	}

	/**
	 * hbase get for rowKeys
	 * 
	 * @param tableName
	 * @param columnFamily
	 * @param rowKeys
	 * @return
	 */
	public JsonArray getArray(String tableName, String columnFamily, List<String> rowKeys) {
		JsonArray array = new JsonArray();
		JsonObject innerObject = new JsonObject();
		byte[] TABLE_NAME = Bytes.toBytes(tableName);
		try {
			Table table = HBaseConnection.getTable(TableName.valueOf(TABLE_NAME));

			List<Get> gets = new ArrayList<>();
			for (String rowKey : rowKeys) {
				Get g = new Get(Bytes.toBytes(rowKey));
				gets.add(g);
			}

			Result[] rs = table.get(gets);
			if (rs == null || rs.length == 0)
				return array;
			for (Result r : rs) {
				if (r == null)
					continue;
				innerObject = get(r, columnFamily);
				if (innerObject != null)
					array.add(innerObject);
			}
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return array;
	}

	/**
	 * hbase get for columnQualifiers
	 * 
	 * @param tableName
	 * @param columnFamily
	 * @param qualifier
	 * @param rowKey
	 * @return
	 */
	public JsonObject get(String tableName, String columnFamily, List<String> columnQualifiers, String rowKey) {
		JsonObject innerObject = new JsonObject();
		byte[] TABLE_NAME = Bytes.toBytes(tableName);
		try {
			Table table = HBaseConnection.getTable(TableName.valueOf(TABLE_NAME));

			Get g = new Get(Bytes.toBytes(rowKey));
			for (String qualifier : columnQualifiers) {
				g.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier));
			}

			Result rs = table.get(g);
			if (rs == null)
				return (null);
			if (!rs.isEmpty()) {
				innerObject = get(rs, columnFamily);
			}
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return innerObject;
	}

	/**
	 * hbase get for columnQualifiers and rowKeys
	 * 
	 * @param tableName
	 * @param columnFamily
	 * @param columnQualifiers
	 * @param rowKeys
	 * @return
	 */
	public List<JsonObject> get(String tableName, String columnFamily, List<String> columnQualifiers,
			List<String> rowKeys) {
		List<JsonObject> list = new ArrayList<>();
		JsonObject innerObject = new JsonObject();
		byte[] TABLE_NAME = Bytes.toBytes(tableName);
		try {
			Table table = HBaseConnection.getTable(TableName.valueOf(TABLE_NAME));

			List<Get> gets = new ArrayList<>();
			for (String rowKey : rowKeys) {
				Get g = new Get(Bytes.toBytes(rowKey));
				for (String qualifier : columnQualifiers) {
					g.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier));
				}
				gets.add(g);
			}

			Result[] rs = table.get(gets);
			if (rs == null || rs.length == 0)
				return list;
			for (Result r : rs) {
				if (r == null)
					continue;
				innerObject = get(r, columnFamily);
				list.add(innerObject);
			}

		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return list;
	}

	/**
	 * hbase get by one qualifier
	 * 
	 * @param tableName
	 * @param columnFamily
	 * @param qualifier
	 * @param rowKey
	 * @return
	 */
	public JsonObject get(String tableName, String columnFamily, String columnQualifier, String rowKey) {
		return get(tableName, columnFamily, Arrays.asList(new String[] { columnQualifier }), rowKey);
	}

	private JsonObject get(Result rs, String columnFamily) {
		JsonObject innerObject = null;
		for (Cell cell : rs.rawCells()) {
			String family = Bytes.toString(CellUtil.cloneFamily(cell));
			if (family.equals(columnFamily)) {
				if (innerObject == null)
					innerObject = new JsonObject();
				byte[] column = CellUtil.cloneQualifier(cell);
				byte[] value = CellUtil.cloneValue(cell);
				String column_name = Bytes.toString(column);
				innerObject.addProperty(column_name, Bytes.toString(value));
			}
		}
		return innerObject;
	}

	public <M> M parser(JsonObject jsonobj, Class<M> mClass) {
		try {
			// Method [] method = mClass.getDeclaredMethods() ;
			Method[] method = mClass.getMethods(); // include all methods
													// extending another
			M entity = mClass.newInstance();
			if (jsonobj == null || jsonobj.isJsonNull())
				return null;
			if (entity != null) {
				for (int i = 0; i < method.length; i++) {
					String methodName = method[i].getName();
					if (methodName.startsWith("set")) {
						String columnName = method[i].getName().replace("set", "");
						if (columnName.toLowerCase().equals("class"))
							continue; // ignore class name
						String type = method[i].getGenericParameterTypes()[0].toString();
						try {
							if (type.contains("Date")) {
								method[i].invoke(entity,
										DateUtil.getDate(jsonobj.get(columnName).getAsString(), "yyyy-MM-dd HH:mm:ss"));
							} else if (type.contains("Long") || type.contains("long")) {
								method[i].invoke(entity,
										isJsonEmpty(jsonobj.get(columnName)) ? 0 : jsonobj.get(columnName).getAsLong());
							} else if (type.contains("Double") || type.contains("double")) {
								method[i].invoke(entity, isJsonEmpty(jsonobj.get(columnName)) ? 0.0
										: jsonobj.get(columnName).getAsDouble());
							} else if (type.contains("Integer") || type.contains("int")) {
								method[i].invoke(entity,
										isJsonEmpty(jsonobj.get(columnName)) ? 0 : jsonobj.get(columnName).getAsInt());
							} else {
								method[i].invoke(entity, isJsonEmpty(jsonobj.get(columnName)) ? ""
										: jsonobj.get(columnName).getAsString());
							}
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				}
			}
			return entity;
		} catch (Exception e) {
			StringWriter sw = new StringWriter();
			e.printStackTrace(new PrintWriter(sw));
			logger.error("TreatHBaseEntry error" + sw.toString());
			return null;
		}
	}

	private boolean isJsonEmpty(JsonElement e) {
		if (e == null)
			return true;
		if (e.getAsString().equals("null"))
			return true;
		return false;
	}

	/**
	 * hbase scan single column
	 * 
	 * @param tableName
	 * @param family
	 * @param qualifier
	 * @param value
	 * @param limit
	 * @return
	 */
	public JsonArray scanSingleColumn(String tableName, String family, String qualifier, String value, int limit) {
		JsonArray collection = new JsonArray();
		try {
			Table table = HBaseConnection.getTable(TableName.valueOf(tableName));
			Scan scan = new Scan();
			// family, qualifier, compareOp, new BinaryComparator(value)

			FilterList filterList = new FilterList();
			Filter filter = new SingleColumnValueFilter(Bytes.toBytes(family), Bytes.toBytes(qualifier),
					CompareOp.EQUAL, Bytes.toBytes(value));
			filterList.addFilter(filter);
			// org.apache.hadoop.hbase.filter.Filter filter = new
			// ValueFilter(CompareOp valueCompareOp,final ByteArrayComparable
			// valueComparator);

			scan.setFilter(filterList);
			scan.addFamily(Bytes.toBytes(family));
			// scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
			scan.setCaching(1000);
			ResultScanner scanner = table.getScanner(scan);
			int count = 0;
			try {
				for (Result result : scanner) {
					JsonObject innerObject = get(result, family);
					if (innerObject != null) {
						if (limit > 0 && ++count > limit)
							break;
						collection.add(innerObject);
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				scanner.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {

		}

		return collection;
	}

	/**
	 * hbase scan column
	 * 
	 * @param tableName
	 * @param family
	 * @param qualifierValueMapping
	 * @param limit
	 * @return
	 */
	public JsonArray scanColumn(String tableName, String family, Map<String, String> qualifierValueMapping, int limit) {
		JsonArray collection = new JsonArray();
		try {
			Table table = HBaseConnection.getTable(TableName.valueOf(tableName));
			Scan scan = new Scan();

			FilterList filterList = new FilterList();
			for (String qualifier : qualifierValueMapping.keySet()) {
				Filter filter = new SingleColumnValueFilter(Bytes.toBytes(family), Bytes.toBytes(qualifier),
						CompareOp.EQUAL, Bytes.toBytes(qualifierValueMapping.get(qualifier)));
				filterList.addFilter(filter);
			}
			// filterList.addFilter(new FirstKeyOnlyFilter()); count
			scan.setFilter(filterList);
			scan.addFamily(Bytes.toBytes(family));
			// scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
			scan.setCaching(1000);
			scan.setCacheBlocks(false);
			// scan.setAllowPartialResults(true);
			ResultScanner scanner = table.getScanner(scan);

			int count = 0;
			try {
				for (Result result : scanner) {
					JsonObject innerObject = get(result, family);
					if (innerObject != null) {
						if (limit > 0 && ++count > limit)
							break;
						collection.add(innerObject);
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				scanner.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {

		}

		return collection;
	}

	/**
	 * hbase scan prefix of rowkey
	 * 
	 * @param tableName
	 * @param family
	 * @param prefixOfRowKey
	 * @param limit
	 * @return
	 */
	public JsonArray scanPrefixOfRowKey(String tableName, String family, String prefixOfRowKey, int limit) {
		JsonArray collection = new JsonArray();
		try {
			Table table = HBaseConnection.getTable(TableName.valueOf(tableName));
			Scan scan = new Scan();

			Filter filter = new PrefixFilter(Bytes.toBytes(prefixOfRowKey));
			scan.setFilter(filter);
			scan.addFamily(Bytes.toBytes(family));
			// scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
			scan.setCaching(1000);
			scan.setCacheBlocks(false);
			ResultScanner scanner = table.getScanner(scan);

			int count = 0;
			try {
				for (Result result : scanner) {
					JsonObject innerObject = get(result, family);
					if (innerObject != null) {
						if (limit > 0 && ++count > limit)
							break;
						collection.add(innerObject);
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				scanner.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {

		}

		return collection;
	}

	/**
	 * parser jsonArray to List<object>
	 * 
	 * @param jsonArray
	 * @param mClass
	 * @return
	 */
	public <M> List<M> parserList(JsonArray jsonArray, Class<M> mClass) {
		List<M> list = new ArrayList<>();
		for (JsonElement e : jsonArray) {
			list.add(parser(e.getAsJsonObject(), mClass));
		}
		return list;
	}

	/**
	 * parser jsonstring JsonObject to List<object>
	 * 
	 * @param obj
	 * @return
	 */
	public <M> List<M> parserJsonstring(JsonObject obj) {
		String jsonstring = obj.get("jsonstring").getAsString();
		// System.out.println("output json:"+jsonstring);

		Type listType = new TypeToken<ArrayList<M>>() {
		}.getType();
		List<M> list = new Gson().fromJson(jsonstring, listType);
		return list;
	}

	/**
	 * hbase get for single column
	 * 
	 * @param tableName
	 * @param family
	 * @param column
	 * @param RowKey
	 * @return
	 */
	public JsonArray GetValueBySingleColumn(String tableName, String family, String column, String RowKey) {
		JsonArray collection = new JsonArray();
		try {
			Table table = HBaseConnection.getTable(TableName.valueOf(tableName));
			Result getResult = table.get(new Get(Bytes.toBytes(RowKey)));
			String result = Bytes.toString(getResult.getValue(Bytes.toBytes(family), Bytes.toBytes(column)));
			Gson gson = new Gson();
			collection = gson.fromJson(result, JsonArray.class);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {

		}

		return collection;
	}

	/**
	 * hbase scan column
	 * 
	 * @param tableName
	 * @param family
	 * @param limit
	 * @return
	 */
	public JsonArray scanColumn(String tableName, String family, int limit) {
		JsonArray collection = new JsonArray();
		try {
			Table table = HBaseConnection.getTable(TableName.valueOf(tableName));
			Scan scan = new Scan();

			scan.addFamily(Bytes.toBytes(family));
			scan.setCaching(1000);
			scan.setCacheBlocks(false);
			// scan.setAllowPartialResults(true);
			ResultScanner scanner = table.getScanner(scan);

			int count = 0;
			try {
				for (Result result : scanner) {
					JsonObject innerObject = get(result, family);
					if (innerObject != null) {
						if (limit > 0 && ++count > limit)
							break;
						collection.add(innerObject);
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				scanner.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {

		}

		return collection;
	}

	/**
	 * hbase row count
	 * 
	 * @param tableName
	 * @param family
	 * @return
	 * @throws Throwable
	 */
	public long rowCountFamily(String tableName, String family) throws Throwable {
		Configuration configuration = HBaseConnection.getConfiguration();
		configuration.setLong("hbase.rpc.timeout", 600000);
		AggregationClient aggregationClient = new AggregationClient(configuration);
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes(family));
		long rowCount = aggregationClient.rowCount(TableName.valueOf(tableName), new LongColumnInterpreter(), scan);
		return rowCount;
	}

	public long rowCountFamily(String tableName, String family, String qualifier) throws Throwable {
		Configuration configuration = HBaseConnection.getConfiguration();
		configuration.setLong("hbase.rpc.timeout", 600000);
		AggregationClient aggregationClient = new AggregationClient(configuration);
		Scan scan = new Scan();
		// scan.addFamily(Bytes.toBytes(family));
		scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));

		long rowCount = aggregationClient.rowCount(TableName.valueOf(tableName), new LongColumnInterpreter(), scan);
		return rowCount;
	}

	public long rowCountFamilyByTimeRange(String tableName, String family, Long timeStart, Long timeEnd)
			throws Throwable {
		Configuration configuration = HBaseConnection.getConfiguration();
		configuration.setLong("hbase.rpc.timeout", 600000);
		AggregationClient aggregationClient = new AggregationClient(configuration);
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes(family));
		// Long timeStart = DateUtil.getDate("2018-08-22 00:00:00").getTime();
		// Long timeEnd = DateUtil.getDate(DateUtil.getDateTime(new Date(), "yyyy-MM-dd
		// HH:mm:ss")).getTime();
		logger.info("time range:" + timeStart + "~" + timeEnd);
		scan.setTimeRange(timeStart, timeEnd);
		long rowCount = aggregationClient.rowCount(TableName.valueOf(tableName), new LongColumnInterpreter(), scan);
		return rowCount;
	}

	public long rowCountFamilyByTimeRange(String tableName, String family, String qualifier) throws Throwable {
		Configuration configuration = HBaseConnection.getConfiguration();
		configuration.setLong("hbase.rpc.timeout", 600000);
		AggregationClient aggregationClient = new AggregationClient(configuration);
		Scan scan = new Scan();
		scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
		Long timeStart = DateUtil.getDate("2019-01-02 00:00:00").getTime();
		Long timeEnd = DateUtil.getDate(DateUtil.getDateTime(new Date(), "yyyy-MM-dd HH:mm:ss")).getTime();
		logger.info("time range:" + timeStart + "~" + timeEnd);
		scan.setTimeRange(timeStart, timeEnd);
		long rowCount = aggregationClient.rowCount(TableName.valueOf(tableName), new LongColumnInterpreter(), scan);
		return rowCount;
	}

}
