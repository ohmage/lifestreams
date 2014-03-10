package lifestreams.models;

import java.io.IOException;

import org.joda.time.DateTime;
import org.ohmage.models.OhmageUser;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.joda.JodaModule;

@JsonIgnoreProperties(ignoreUnknown = true)
public class StreamRecord<T> {

	public StreamRecord() {
	}

	private OhmageUser user;
	private StreamMetadata metadata = new StreamMetadata();
	private T data;

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
		mapper.configure(
				com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS,
				false);

		ObjectNode node = mapper.convertValue(this, ObjectNode.class);
		return node;
	}

	public static class StreamRecordFactory<T> {
		Class<T> c;

		public StreamRecordFactory(Class<T> c) {
			this.c = c;
		}

		public StreamRecord<T> createRecord(ObjectNode node, OhmageUser user)
				throws JsonParseException, JsonMappingException, IOException {
			ObjectMapper mapper = new ObjectMapper();
			mapper.registerModule(new JodaModule());
			mapper.configure(
					com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,
					false);
			// use the timezone in the timestamp field to parse the datatime
			// string
			mapper.setTimeZone(new DateTime(node.get("metadata")
					.get("timestamp").asText()).getZone().toTimeZone());
			@SuppressWarnings("unchecked")
			StreamRecord<T> dataPoint = mapper.convertValue(node, new StreamRecord<T>().getClass());
			dataPoint.setData(mapper.convertValue(dataPoint.getData(), c));
			dataPoint.setUser(user);
			return dataPoint;
		}
	}

}
