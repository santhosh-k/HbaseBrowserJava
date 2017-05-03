package main;

import java.io.IOException;
import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;

import dao.HBaseDao;

@Path("/hbaseService")
public class UserService {
	@GET
	@Path("/getTables")
	@Produces(MediaType.APPLICATION_JSON)
	public String getTables() throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		List<String> string = new HBaseDao().getTables();
		return string.toString();
	}
}