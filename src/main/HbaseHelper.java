package main;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import commons.Constants;

public class HbaseHelper {

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
}
