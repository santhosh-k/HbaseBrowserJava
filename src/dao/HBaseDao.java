package dao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.map.ObjectMapper;

import commons.CommonUtils;
import commons.Constants;

public class HBaseDao {
	
	public Map<String , Object> getHbaseTables(HBaseAdmin admin) throws IOException {
		Map<String , Object> tableMap = new HashMap<>();
		
		List<String> tableList = new ArrayList<String>();

		// Getting all the list of tables using HBaseAdmin object
		HTableDescriptor[] htabelList = admin.listTables();

		// printing all the table names.
		for (HTableDescriptor htableDescriptor : htabelList) {
			String hTableName = htableDescriptor.getNameAsString();
			if (hTableName.contains("_")) {
				continue;
			}
			tableList.add(hTableName);
		}
		
		tableMap.put(Constants.DATA,tableList);
		
		return tableMap;
	}
	
	/**
	 * Adds the row.
	 *
	 * @param uniqueRowKey
	 *            the unique row key
	 * @param data
	 *            the data
	 * @param fieldsToIgnore
	 *            the fields to ignore
	 * @throws IOException 
	 */
	public void addRow(String uniqueRowKey, Object data,
			List<String> fieldsToIgnore, String columnFamily,String tableName , HBaseAdmin admin)
			throws IOException {
		Put put = generatePut(uniqueRowKey, columnFamily, data, fieldsToIgnore,tableName);
		addRow(put,admin,tableName);
	}
	
	/**
	 * Generate put.
	 *
	 * @param uniqueRowKey
	 *            the unique row key
	 * @param data
	 *            the data
	 * @param fieldsToIgnore
	 *            the fields to ignore
	 * @return the put
	 */
	@SuppressWarnings({ "deprecation", "unchecked" })
	public Put generatePut(String uniqueRowKey, String columnFamily,
			Object data, List<String> fieldsToIgnore,String tableName) {
		ObjectMapper objectMapper = new ObjectMapper();
		Map<String, Object> dataMap = objectMapper.convertValue(data,
				HashMap.class);
		Put put = new Put(Bytes.toBytes(uniqueRowKey));
		for (Map.Entry<String, Object> entry : dataMap.entrySet()) {
			String key = entry.getKey();
			if (fieldsToIgnore != null && fieldsToIgnore.contains(key)) {
				continue;
			}
			Object value = entry.getValue();
			if (value == null) {
				value = "";
			}
			if (columnFamily != null) {
				columnFamily = tableName;
			}
			put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(key),
					CommonUtils.convertIntoBytes(value));
		}
		return put;
	}
	
	
	/**
	 * Adds the row.
	 *
	 * @param put
	 *            the put
	 * @throws IOException 
	 */
	@SuppressWarnings("deprecation")
	public void addRow(Put put,HBaseAdmin hbaseAdmin,String tableName) throws IOException {
		HTable hTable = new HTable(hbaseAdmin.getConfiguration(), tableName);
		hTable.put(put);
		hTable.close();
	}
	
}