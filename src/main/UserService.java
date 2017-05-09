package main;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;

import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import commons.CommonUtils;
import commons.Constants;

@Path("/hbaseService")
public class UserService {
		
	/**
	 * CONFIGURATION holds the unique configuration of hbase with its individual
	 * paramters in it.
	 */
	private static Configuration configuration;

	/**
	 * admin holds the unique hbase admin which is used to implement hbase table
	 * level configuration.
	 */
	private static HBaseAdmin admin = null;

	private static Map<String, Map<String, String>> sessionMap = new HashMap<String, Map<String, String>>();

	@POST
	@Path("/connect")
	public Response connectToHbase(String connectionJson) {

		Response response = null;

		Map<String, Object> dataMap = new HashMap<>();
		String sessionId = "sess";
		try {
			// String sessionId = request.getSession().getId();
			JSONObject connectionObject = (JSONObject) JSONValue
					.parse(connectionJson);

			JSONObject data = (JSONObject) connectionObject.get(Constants.DATA);

			String ipAddress = (String) data.get(Constants.IPADDRESS);

			String apiKey = (String) data.get(Constants.API_KEY);

			if (sessionMap.containsKey(sessionId)) {
				Map<String, String> valueMap = sessionMap.get(sessionId);
				apiKey = valueMap.get(Constants.API_KEY);
				ipAddress = valueMap.get(Constants.IPADDRESS);
			} else {
				Map<String, String> valueMap = new HashMap<String, String>();
				valueMap.put(Constants.IPADDRESS, ipAddress);
				valueMap.put(Constants.API_KEY, apiKey);
				sessionMap.put(sessionId, valueMap);
				admin = getHbaseConnection(ipAddress);

			}

			if (admin != null) {
				Map<String, String> successMap = new HashMap<String, String>();
				successMap.put(Constants.DATA, Constants.CONNECTED);
				dataMap.put(Constants.SUCCESS, successMap);

				response = CommonUtils.buildSuccessResponse(dataMap);

			} else {
				Map<String, String> failureMap = new HashMap<String, String>();
				failureMap.put(Constants.MESSAGE, "Unable to connect");
				dataMap.put(Constants.ERROR, failureMap);
				response = CommonUtils.buildSuccessResponse(dataMap);
			}
		} catch (IOException e) {

			response = CommonUtils.buildFailureResponse(e.getMessage());
		}

		return response;

	}

	public static HBaseAdmin getHbaseConnection(String ipAddress) {
		try {
			if (admin == null) {

				// New Instance of Hbase
				configuration = HBaseConfiguration.create();
				// Zookeeper Ip
				configuration.set(
						Constants.HBASE_CONFIGURATION_ZOOKEEPER_QUORUM,
						ipAddress);
				// Zookeeper Port
				configuration.set(
						Constants.HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT,
						"2181");
				admin = new HBaseAdmin(configuration);

			}
		} catch (Exception e) {
			e.printStackTrace();
			return null ;
		}
		return admin;
	}

	@GET
	@Path("/getTables")
	public Response getHtables(@QueryParam(Constants.SESSIONID) String sessionId) {

		Response response = null;
		
		Map<String, String> valueMap = sessionMap.get(sessionId);
		if(valueMap == null){
			response = CommonUtils.buildFailureResponse("Please connect to hbase first");
			return response;
		}
		String ipAddress = valueMap.get(Constants.IPADDRESS);
		HBaseAdmin	hbaseAdmin = getHbaseConnection(ipAddress);

		HbaseHelper hbaseHelper = getHhbaseHelper();

		try {
			Map<String, Object> tableMap = hbaseHelper.getHbaseTables(hbaseAdmin);
			response = CommonUtils.buildSuccessResponse(tableMap);
			return response;

		} catch (IOException e) {
			response = CommonUtils.buildFailureResponse(e.getMessage());
		}

		return response;

	}

	private HbaseHelper getHhbaseHelper() {
		return new HbaseHelper();
	}
}