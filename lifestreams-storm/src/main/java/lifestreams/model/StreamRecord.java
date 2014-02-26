package lifestreams.model;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

import org.joda.time.DateTime;
import org.ohmage.models.OhmageStream;
import org.ohmage.models.OhmageUser;

import co.nutrino.api.moves.impl.response.serialize.MovesTimezoneDeserializer;

import com.bbn.openmap.geo.Geo;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.joda.JodaModule;
@JsonIgnoreProperties(ignoreUnknown = true)
public class StreamRecord implements IDataPoint {

	@JsonIgnoreProperties(ignoreUnknown = true)
	public static class Metadata {
        private DateTime _timestamp;
        private GeoLocation _location;
        
        public DateTime getTimestamp() { return _timestamp; }
        @JsonInclude(Include.NON_NULL)
        @JsonSerialize(using = GeoLocation.GeoLocationSerializer.class)
        public GeoLocation getLocation() { return _location; }
  
        public void setTimestamp(DateTime t) { _timestamp = t; }
        
        @JsonDeserialize(using = GeoLocation.GeoLocationDeserializer.class)
        public void setLocation(GeoLocation l) { _location = l; }
	}
	public StreamRecord(){}
	private OhmageUser user;
	private Metadata metadata = new Metadata();
	@Override
	@JsonIgnore
	public DateTime getTimestamp() {
		return this.metadata.getTimestamp();
	}
	@JsonIgnore
	public GeoLocation getLocation() {
		return this.metadata.getLocation();
	}
	
	@Override
	@JsonIgnore
	public OhmageUser getUser() {
		return user;
	}
	@JsonIgnore
	public void setTimestamp(DateTime timestamp) {
		this.metadata.setTimestamp(timestamp);
	}
	@JsonIgnore
	public void setUser(OhmageUser user) {
		this.user = user;
	}
	@JsonIgnore
	public void setLocation(GeoLocation location) {
		this.metadata.setLocation(location);
	}
	public Metadata getMetadata() {
		return metadata;
	}
	public void setMetadata(Metadata metadata) {
		this.metadata = metadata;
	}
	public StreamRecord(OhmageUser user, DateTime timestamp){
		this.user = user;
		this.setTimestamp(timestamp);
	}
	public ObjectNode toObserverDataPoint(){
		 ObjectMapper mapper = new ObjectMapper();
		 mapper.registerModule(new JodaModule());
		 mapper.configure(com.fasterxml.jackson.databind.SerializationFeature.
				WRITE_DATES_AS_TIMESTAMPS , false);
		 
		ObjectNode node = mapper.convertValue(this, ObjectNode.class);
		Iterator<String> fieldNameIter = node.fieldNames();
		ObjectNode dataNode = mapper.createObjectNode();
		ArrayList<String> dataFields = new ArrayList<String>();
		while(fieldNameIter.hasNext()){
			String fieldName = fieldNameIter.next();
			if(!fieldName.equals("metadata")){
				dataFields.add(fieldName);
			}
		}
		for(String fieldName: dataFields){
			dataNode.put(fieldName, node.get(fieldName));
			node.remove(fieldName);
		}
		
			
		
		node.put("data", dataNode);
		// if location is null
		return node;
	}
	public static class OhmageStreamDataPointFactory<T extends StreamRecord>{
		Class<T> c;
		public OhmageStreamDataPointFactory(Class<T> c){
			this.c = c;
		}
		public T createDataPoint(ObjectNode node, OhmageUser user) throws JsonParseException, JsonMappingException, IOException{
			 ObjectMapper mapper = new ObjectMapper();
			 mapper.registerModule(new JodaModule());
			 mapper.configure(com.fasterxml.jackson.databind.SerializationFeature.
					WRITE_DATES_AS_TIMESTAMPS , false);
			 mapper.setTimeZone(new DateTime(node.get("metadata").get("timestamp").asText()).getZone().toTimeZone());
			ObjectNode dataNode = (ObjectNode) node.get("data");
			Iterator<String> fieldNameIter = dataNode.fieldNames();
			while(fieldNameIter.hasNext()){
				String fieldName = fieldNameIter.next();
				node.put(fieldName, dataNode.get(fieldName));
			}
			node.remove("data");
			T dataPoint = mapper.convertValue(node, c);
			dataPoint.setUser(user);
			return dataPoint;
		}
	}
	

}
