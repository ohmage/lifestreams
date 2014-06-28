package org.ohmage.lifestreams.models;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.*;
import com.javadocmd.simplelatlng.LatLng;
import com.javadocmd.simplelatlng.LatLngTool;
import com.javadocmd.simplelatlng.util.LengthUnit;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.IOException;

public class GeoLocation {
	// The ISO8601-formatted date-time-timezone string.
	private final DateTime timestamp;
	// geo coordinates
	private final LatLng coordinates;
	// accuracy in meter
	private final double accuracy;
	// name of provider (WiFi, GPS, etc)
	private final String provider;

    /**
     *
     * GeoLocation constructor with accuracy
     * @param timestamp when the location acquired
     * @param coordinates LatLng
     * @param accuracy accuracy radius (in meters)
     * @param provider where this geolocation come from
     */
	public GeoLocation(DateTime timestamp, LatLng coordinates, double accuracy,
			String provider) {
		super();
		this.timestamp = timestamp;
		this.coordinates = coordinates;
		this.accuracy = accuracy;
		this.provider = provider;
	}

    /**
     * GeoLocation constructor for the cases that accuracy is not applicable
     * @param timestamp when the location acquired
     * @param coordinates LatLng
     * @param provider where this geolocation come from
     */
    public GeoLocation(DateTime timestamp, LatLng coordinates,
                       String provider) {
        super();
        this.timestamp = timestamp;
        this.coordinates = coordinates;
        this.accuracy = -1;
        this.provider = provider;
    }
	public DateTime getTimestamp() {
		return timestamp;
	}

	public LatLng getCoordinates() {
		return coordinates;
	}

	public double getAccuracy() {
		return accuracy;
	}

	public String getProvider() {
		return provider;
	}
	public String toString(){
		return String.format("%s accuracy:%s time:%s", this.getCoordinates().toString(), this.getAccuracy(), this.getTimestamp());
	}
	public static double distance(GeoLocation x, GeoLocation y, LengthUnit unit){
		return LatLngTool.distance(x.coordinates, y.coordinates, unit);
	}
	public static class GeoLocationDeserializer extends
			JsonDeserializer<GeoLocation> {
		@Override
		public GeoLocation deserialize(JsonParser jp,
				DeserializationContext ctxt) throws
                IOException {
			ObjectCodec oc = jp.getCodec();
			JsonNode node = oc.readTree(jp);

			LatLng geo = new LatLng(node.get("latitude").asDouble(), node.get(
					"longitude").asDouble());
			DateTime timestamp = null;
			if (node.get("timestamp") != null) {
				timestamp = new DateTime(node.get("timestamp").asText());
			} else if (node.get("time") != null && node.get("timezone") != null) {
				timestamp = new DateTime(node.get("time").asLong(),
						DateTimeZone.forID(node.get("timezone").asText()));
			}
			double accuracy = node.get("accuracy").asDouble();
			String provider = node.get("provider").asText();
			return (new GeoLocation(timestamp, geo, accuracy, provider));
		}
	}

	public static class GeoLocationSerializer extends
			JsonSerializer<GeoLocation> {
		@Override
		public void serialize(GeoLocation value, JsonGenerator jgen,
				SerializerProvider provider) throws IOException {
			jgen.writeNumberField("latitude", value.coordinates.getLatitude());
			jgen.writeNumberField("longitude", value.coordinates.getLongitude());
			jgen.writeStringField("provider", value.provider);
			jgen.writeStringField("timestamp", value.timestamp.toString());
			jgen.writeNumberField("accuracy", value.accuracy);
		    jgen.writeEndObject();

		}
	}
}
