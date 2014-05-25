package org.ohmage.lifestreams.models;

import java.io.IOException;

import org.joda.time.DateTime;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@JsonIgnoreProperties(ignoreUnknown = true)
public class StreamMetadata {

	private DateTime _timestamp;
	private GeoLocation _location;
	
	@JsonSerialize(using = DateTimeTextSerializer.class)
	public DateTime getTimestamp() {
		return _timestamp;
	}
	
	
	// serialize timestamp field as string instead of Calendar object
	static class DateTimeTextSerializer extends
			JsonSerializer<DateTime> {

		@Override
		public void serialize(DateTime value, JsonGenerator jgen,
				SerializerProvider provider) throws IOException,
				JsonProcessingException {
			jgen.writeString(value.toString());
			
		}
	}

	
	@JsonInclude(Include.NON_NULL)
	@JsonSerialize(using = GeoLocation.GeoLocationSerializer.class)
	public GeoLocation getLocation() {
		return _location;
	}

	public void setTimestamp(DateTime t) {
		_timestamp = t;
	}

	@JsonDeserialize(using = GeoLocation.GeoLocationDeserializer.class)
	public void setLocation(GeoLocation l) {
		_location = l;
	}
}
