package org.ohmage.lifestreams.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.joda.time.DateTime;

public class StreamMetadata {

    private DateTime _timestamp;
    private GeoLocation _location;

    public DateTime getTimestamp() {
        return _timestamp;
    }

    public void setTimestamp(DateTime t) {
        _timestamp = t;
    }


    @JsonInclude(Include.NON_NULL)
    @JsonSerialize(using = GeoLocation.GeoLocationSerializer.class)
    public GeoLocation getLocation() {
        return _location;
    }


    @JsonDeserialize(using = GeoLocation.GeoLocationDeserializer.class)
    public void setLocation(GeoLocation l) {
        _location = l;
    }
}
