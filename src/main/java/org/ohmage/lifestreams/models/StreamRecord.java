package org.ohmage.lifestreams.models;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;
import org.ohmage.models.OhmageUser;

import java.io.IOException;
import java.io.Serializable;
import java.util.TimeZone;

@JsonIgnoreProperties(ignoreUnknown = true)
public class StreamRecord<T> implements Comparable{
    static ObjectMapper mapper = new ObjectMapper();
    {
        // register datetime serializer
        mapper.registerModule(new DateTimeSerializeModule());
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
	}

    // alias for setData
	public void d(T data) {
		this.data = data;
	}

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
		// set the timezone of the data point as the timestamp's timezone
		ObjectNode node = mapper.convertValue(this, ObjectNode.class);
		return node;
	}

	public static class StreamRecordFactory implements Serializable {
		public StreamRecordFactory(){
		}
		public <T> StreamRecord<T> createRecord(ObjectNode node, OhmageUser user, Class<T> dataClass)
				throws IOException {
			StreamRecord<T> dataPoint = mapper.convertValue(node, new StreamRecord<T>().getClass());
			dataPoint.setData(mapper.convertValue(dataPoint.getData(), dataClass));
			dataPoint.setUser(user);
			return dataPoint;
		}
	}

	@Override
	public int compareTo(Object arg0) {
		// by default, sort StreamRecord by time
		return (int) (this.getTimestamp().getMillis() - ((StreamRecord) arg0).getTimestamp().getMillis());
	}
    public StreamRecord() {
    }
}
