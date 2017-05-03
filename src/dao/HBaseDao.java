package dao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class HBaseDao {
	public static final String HBASE_CONFIGURATION_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
	public static final String HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT = "hbase.zookeeper.property.clientPort";
	HBaseAdmin admin = null;
	public HBaseDao() throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		Configuration hConf = HBaseConfiguration.create();
		hConf.set(HBASE_CONFIGURATION_ZOOKEEPER_QUORUM, "127.0.0.1");
		hConf.setInt(HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT, 2181);
		admin = new HBaseAdmin(hConf);
	}

	public List<String> getTables() throws IOException{
		List<String> response = new ArrayList<String>();
		TableName[] tables = admin.listTableNames();
		for (TableName tableName : tables) {
			response.add(tableName.getNameAsString());
		}
		return response;
	}

}
