# hbasestarter
## 說明:
為了方便使用 HBase, 將常用的方法整合在這包 

## 使用方式
### 初始化連線:
    new HBaseClient().init();
    or
    new HBaseClient().init(path);

### 讀資料:
    HBaseClient.getInstance().get(tableName, columnFamily, rowKey);
	HBaseClient.getInstance().get(tableName, columnFamily, rowKeys);
	HBaseClient.getInstance().getArray(tableName, columnFamily, rowKeys);

### 寫資料:
    HBaseClient.getInstance().postEntryMutator(tableName, columnFamily, mClass, map);
	HBaseClient.getInstance().postEntryMutator(tableName, columnFamily, mClass, map, subRowKeyNames);
	HBaseClient.getInstance().postStringValueByMultiColumn(tableName, columnFamily, map);
    HBaseClient.getInstance().postStringValueBySingleColumn(tableName, columnFamily, columnName, map);
