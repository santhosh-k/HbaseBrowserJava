package main;

import java.io.IOException;
import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.CookieParam;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Cookie;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import commons.CommonUtils;
import commons.Constants;
import dao.HBaseDao;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;

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

	/**
	 * The static map that holds the session.
	 */

	private static Map<String, Map<String, String>> sessionMap = new HashMap<String, Map<String, String>>();

	/**
	 * The service that register the user who enters into the application.
	 * 
	 * @param connectionJson
	 * @return
	 */
	@POST
	@Path("/connect")
	@Produces(MediaType.APPLICATION_JSON)
	public Response connectToHbase(
			@HeaderParam(value = "sessionId") String sessionId,
			String connectionJson) {

		Response response = null;

		Map<String, Object> dataMap = new HashMap<>();
		try {
			JSONObject connectionObject = (JSONObject) JSONValue
					.parse(connectionJson);

			JSONObject data = (JSONObject) connectionObject.get(Constants.DATA);

			String ipAddress = (String) data.get(Constants.IPADDRESS);

			String apiKey = (String) data.get(Constants.API_KEY);

			Map<String, String> valueMap = new HashMap<String, String>();

			if (sessionId != null) {
				if (sessionMap.containsKey(sessionId)) {
					valueMap = sessionMap.get(sessionId);
					apiKey = valueMap.get(Constants.API_KEY);
					ipAddress = valueMap.get(Constants.IPADDRESS);
				} else {
					valueMap.put(Constants.IPADDRESS, ipAddress);
					valueMap.put(Constants.API_KEY, apiKey);
					sessionMap.put(sessionId, valueMap);
					admin = getHbaseConnection(ipAddress);

				}
			} else {
				sessionId = next();
				valueMap.put(Constants.IPADDRESS, ipAddress);
				valueMap.put(Constants.API_KEY, apiKey);
				sessionMap.put(sessionId, valueMap);
				admin = getHbaseConnection(ipAddress);
				dataMap.put(Constants.SESSIONID, sessionId);
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

	/**
	 * 
	 * @param ipAddress
	 * @return
	 */
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
			return null;
		}
		return admin;
	}

	@GET
	@Path("/getTables")
	public Response getHtables(@CookieParam("sessionId") Cookie cookie) {

		Response response = null;

		Map<String, String> valueMap = sessionMap.get(cookie.getValue());
		if (valueMap == null) {
			response = CommonUtils
					.buildFailureResponse("Please connect to hbase first");
			return response;
		}
		String ipAddress = valueMap.get(Constants.IPADDRESS);
		HBaseAdmin hbaseAdmin = getHbaseConnection(ipAddress);


		try {
			Map<String, Object> tableMap = getHbaseDao()
					.getHbaseTables(hbaseAdmin);
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
	
	private HBaseDao getHbaseDao() throws MasterNotRunningException, ZooKeeperConnectionException, IOException{
		return new HBaseDao();
	}

	@POST
	@Path("/disconnect")
	@Produces(MediaType.APPLICATION_JSON)
	public Response logout(@HeaderParam(value = "sessionId") String sessionId) {
		Response response = CommonUtils.buildSuccessResponse("Logged out successfully");
		sessionMap.remove(sessionId);
		
		return response;

	}

	public String next() {
		SecureRandom ran = new SecureRandom();
		return new BigInteger(130, ran).toString(32);
	}
}