package lifestreams.model;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.joda.time.DateTime;

import com.esotericsoftware.kryo.Serializer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;

public class DataPoint {
	OhmageUser _user;
	DateTime _timestamp;
	ObjectNode _data;
	ObjectNode _metadata;
	static ObjectMapper objectMapper = new ObjectMapper();
	public DataPoint(OhmageUser user, DateTime timestamp, ObjectNode data, ObjectNode metadata){
		this._user = user;
		this._timestamp = timestamp;
		this._data = data;
		this._metadata = metadata;
	}
	public DataPoint(OhmageUser user, DateTime timestamp, ObjectNode data){
		ObjectNode metadataNode = objectMapper.createObjectNode();
		metadataNode.put("timestamp", timestamp.toString());
		
		this._user = user;
		this._timestamp = timestamp;
		this._data = data;
		this._metadata = metadataNode;
	}
	public DateTime getTimestamp(){
		return _timestamp;
	}
	public ObjectNode getData(){
		return _data;
	}
	public ObjectNode getMetadata(){
		return _metadata;
	}
	public OhmageUser getUser(){
		return _user;
	}

}
