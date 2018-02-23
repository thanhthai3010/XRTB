package com.aerospike.redisson;

import java.io.IOException;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * The Serialization tool we use for redisson objects.
 * 
 * @author Ben M. Faul
 *
 */
public class Tools {

    /** The JSON mapping object */
    static final ObjectMapper mapper = new ObjectMapper();
    static {
	mapper.setSerializationInclusion(Include.NON_NULL);
	mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    /**
     * Serialize an object. Creates a JSON object, then adds a key 'serialClass',
     * which the deserializer uses to convert back to that object.
     * 
     * @param o
     *            Object. Any object to serialize.
     * @return String. The JSON serialized representation.
     */
    public static String serialize(Object o) {
	String contents = null;
	StringBuilder sb = new StringBuilder();
	try {
	    contents = mapper.writeValueAsString(o);
	    sb.append(contents);
	    sb.setLength(sb.length() - 1);
	    sb.append(",\"");
	    sb.append("serialClass");
	    sb.append("\":\"");
	    sb.append(o.getClass().getName());
	    sb.append("\"}");
	    contents = sb.toString();
	} catch (JsonProcessingException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}

	return contents;
    }

    /**
     * The desrialized object.
     * 
     * @param o
     *            String. The JSON object to deserialize.
     * @return Object []. returns[0] is the full package name of the object,
     *         returns[1] is the object itself.
     */
    public static Object[] deSerialize(String o) {
	Object obj = null;
	String name = null;
	if (o.charAt(0) == '{') {
	    int i = o.indexOf("\"serialClass");
	    StringBuilder sb = new StringBuilder(o);
	    name = sb.substring(i + 15, sb.length() - 2);
	    sb.setLength(i - 1);
	    sb.append("}");
	    String contents = sb.toString();
	    try {
		obj = mapper.readValue(contents, Class.forName(name));
	    } catch (ClassNotFoundException | IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	    }
	} else if (o.charAt(0) == '"') {
	    int i = o.indexOf("\"serialClass");
	    StringBuilder sb = new StringBuilder(o);
	    name = sb.substring(i + 15, sb.length() - 2);
	    obj = sb.substring(1, i - 1);
	} else {
	    int i = o.indexOf("\"serialClass");
	    StringBuilder sb = new StringBuilder(o);
	    name = sb.substring(i + 15, sb.length() - 2);
	    String contents = sb.substring(0, i - 1);
	    try {
		if (name.contains("Double") && contents.endsWith(".")) {
		    sb = new StringBuilder(contents);
		    sb.append("0");
		    contents = sb.toString();
		}
		obj = mapper.readValue(contents, Class.forName(name));
	    } catch (ClassNotFoundException | IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	    }
	}
	Object[] pair = new Object[2];
	pair[0] = name;
	pair[1] = obj;
	return pair;
    }
}
