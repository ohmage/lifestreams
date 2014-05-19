package org.ohmage.lifestreams.models;

import java.io.IOException;
import java.io.Serializable;
import java.util.TimeZone;

import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;
import org.ohmage.models.OhmageUser;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.joda.JodaModule;

@JsonIgnoreProperties(ignoreUnknown = true)
public class StreamRecord<T> implements Comparable{

	public StreamRecord() {
	}

	private OhmageUser user;
	private StreamMetadata metadata = new StreamMetadata();
	private T data;
	public String toString(){
		return String.format("Time:%s\nLocation:%s\nData:%s\n",
				this.getTimestamp(),
				this.getLocation() == null ? "NA" : this.getLocation().toString(),
				this.getData().toString());
		
	}

	public T getData() {
		return data;
	}

	public void setData(T data) {
		this.data = data;
	}

	// alias for getData
	public T d() {
		return data;
	};

	// alias for setData
	public void d(T data) {
		this.data = data;
	};

	@JsonIgnore
	public DateTime getTimestamp() {
		return this.metadata.getTimestamp();
	}

	@JsonIgnore
	public GeoLocation getLocation() {
		return this.metadata.getLocation();
	}

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

	public StreamMetadata getMetadata() {
		return metadata;
	}

	public void setMetadata(StreamMetadata metadata) {
		this.metadata = metadata;
	}

	public StreamRecord(OhmageUser user, DateTime timestamp) {
		this.user = user;
		this.setTimestamp(timestamp);
	}

	public StreamRecord(OhmageUser user, DateTime timestamp,
			GeoLocation location) {
		this.user = user;
		this.setTimestamp(timestamp);
		this.setLocation(location);
	}

	public StreamRecord(OhmageUser user, DateTime timestamp, T data) {
		this.user = user;
		this.setTimestamp(timestamp);
		this.setData(data);
	}

	public StreamRecord(OhmageUser user, DateTime timestamp,
			GeoLocation location, T data) {
		this.user = user;
		this.setTimestamp(timestamp);
		this.setLocation(location);
		this.setData(data);
	}

	public ObjectNode toObserverDataPoint() {
		ObjectMapper mapper = new ObjectMapper();
		mapper.registerModule(new JodaModule());
		// set the timezone of the data point as the timestamp's timezone
		mapper.setTimeZone(this.getTimestamp().getZone().toTimeZone());
		mapper.configure(
				com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS,
				false);

		ObjectNode node = mapper.convertValue(this, ObjectNode.class);
		return node;
	}

	public static class StreamRecordFactory<T> implements Serializable {
		private Class<T> c;
		public static <T> StreamRecordFactory<T> createStreamRecordFactory(Class<T> c ){
			return new StreamRecordFactory<T>(c);
		}
		private StreamRecordFactory(Class<T> c){
			this.c = c;
		}
		public StreamRecord<T> createRecord(ObjectNode node, OhmageUser user)
				throws JsonParseException, JsonMappingException, IOException {
			ObjectMapper mapper = new ObjectMapper();
			
			mapper.registerModule(new JodaModule());
			mapper.configure(
					com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,
					false);
			// use the timezone in the timestamp field to parse the datatime string
			
			// get the timezone
			TimeZone zone = ISODateTimeFormat.dateTime().withOffsetParsed().parseDateTime(node.get("metadata").get("timestamp").asText()).getZone().toTimeZone();

			// make the json parser to use the timezone in the timestamp field to parse all the ISODateTime
			mapper.setTimeZone(zone);

			@SuppressWarnings("unchecked")
			StreamRecord<T> dataPoint = mapper.convertValue(node, new StreamRecord<T>().getClass());
			
			dataPoint.setData(mapper.convertValue(dataPoint.getData(), c));
			dataPoint.setUser(user);
			return dataPoint;
		}
	}

	@Override
	public int compareTo(Object arg0) {
		// by default, sort StreamRecord by time
		return (int) (this.getTimestamp().getMillis() - ((StreamRecord) arg0).getTimestamp().getMillis());
	}

}
