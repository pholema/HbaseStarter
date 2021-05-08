# hbasestarter
## Overview
help you to asscess Hbase data easily.

## Description
### Initial connection:
    new HBaseClient().init();
    or
    new HBaseClient().init(path);

### Read data:
    HBaseClient.getInstance().get(tableName, columnFamily, rowKey);
	HBaseClient.getInstance().get(tableName, columnFamily, rowKeys);
	HBaseClient.getInstance().getArray(tableName, columnFamily, rowKeys);

### Write data:
    HBaseClient.getInstance().postEntryMutator(tableName, columnFamily, mClass, map);
	HBaseClient.getInstance().postEntryMutator(tableName, columnFamily, mClass, map, subRowKeyNames);
	HBaseClient.getInstance().postStringValueByMultiColumn(tableName, columnFamily, map);
    HBaseClient.getInstance().postStringValueBySingleColumn(tableName, columnFamily, columnName, map);
