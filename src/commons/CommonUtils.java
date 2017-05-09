package commons;

import java.io.IOException;
import java.io.Serializable;

import javax.ws.rs.core.Response;

import org.apache.commons.lang3.SerializationUtils;
import org.codehaus.jackson.map.ObjectMapper;

import commons.Constants;

public class CommonUtils {
	
	
	
	/**
	 * This method is used to convert object into bytes.
	 * 
	 * @param object
	 *            holds the unique object to convert as bytes.
	 * @return the byte array to store in db.
	 */
	public static byte[] convertIntoBytes(Object object) {
		byte[] byteData = null;
		byteData = SerializationUtils.serialize((Serializable) object);
		return byteData;
	}
	
	
	/**
	 * This method is used to convert object into bytes.
	 * 
	 * @param object
	 *            holds the unique object to convert as bytes.
	 * @return the byte array to store in db.
	 */
	public static Object convertIntoObject(byte[] byteArray) {
		Object obj = null;
		obj = SerializationUtils.deserialize(byteArray);
		return obj;
	}
	
	/**
	 * Check string is empty or null.
	 * 
	 * @param value
	 *            string value
	 * @return boolean
	 */
	public static boolean isEmptyOrNull(String value) {
		return value == null || value.trim().length() == Constants.ZERO
				|| Constants.NULL_STRING.equalsIgnoreCase(value);
	}
	
	
	/**
	 * Check object is empty or null.
	 * 
	 * @param value
	 *            string value
	 * @return boolean
	 */
	public static boolean isEmptyOrNullObject(Object value) {
		return null == value || Constants.EMPTY.equals(value);
	}

	
	
	/**
	 * Builds the response.
	 * 
	 * @param data
	 *            the data
	 * @return the response
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public static Response buildSuccessResponse(Object data) throws IOException {
		String response = Constants.SUCCESS;
		if (data != null) {
			final ObjectMapper mapper = new ObjectMapper();
			response = mapper.writeValueAsString(data);
		}
		return Response.status(Constants.STATUSOK).entity(response).build();

	}
	
	/**
	 * 
	 * @param message
	 * @return
	 */
	public static Response buildFailureResponse(String message) {
		Response response = Response.status(Constants.FAILURE)
				.entity(message).build();
		return response;
	}
	
	/**
	 * 
	 * @param message
	 * @return
	 */
	public static Response buildSuccessResponse(String message) {
		Response response = Response.status(Constants.STATUSOK)
				.entity(message).build();
		return response;
	}
}
