package org.ohmage.lifestreams.models;

import org.joda.time.DateTime;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@JsonIgnoreProperties(ignoreUnknown = true)
public class StreamMetadata {

	private DateTime _timestamp;
	private GeoLocation _location;

	public DateTime getTimestamp() {
		return _timestamp;
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
