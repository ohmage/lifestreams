package lifestreams.models;

import java.io.IOException;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.bbn.openmap.geo.Geo;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

public class GeoLocation {
	// The ISO8601-formatted date-time-timezone string.
	final DateTime timestamp;
	// geo coordinates
	final Geo coordinates;
	// accuracy in meter
	final double accuracy;
	// name of provider (WiFi, GPS, etc)
	final String provider;

	public GeoLocation(DateTime timestamp, Geo coordinates, double accuracy,
			String provider) {
		super();
		this.timestamp = timestamp;
		this.coordinates = coordinates;
		this.accuracy = accuracy;
		this.provider = provider;
	}

	public DateTime getTimestamp() {
		return timestamp;
	}

	public Geo getCoordinates() {
		return coordinates;
	}

	public double getAccuracy() {
		return accuracy;
	}

	public String getProvider() {
		return provider;
	}

	public static class GeoLocationDeserializer extends
			JsonDeserializer<GeoLocation> {
		@Override
		public GeoLocation deserialize(JsonParser jp,
				DeserializationContext ctxt) throws JsonProcessingException,
				IOException {
			ObjectCodec oc = jp.getCodec();
			JsonNode node = oc.readTree(jp);

			Geo geo = new Geo(node.get("latitude").asDouble(), node.get(
					"longitude").asDouble(), true);
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
				SerializerProvider provider) throws IOException,
				JsonProcessingException {
			jgen.writeNumberField("latitude", value.coordinates.getLatitude());
			jgen.writeNumberField("longitude", value.coordinates.getLongitude());
			jgen.writeStringField("provider", value.provider);
			jgen.writeStringField("timestamp", value.timestamp.toString());
			jgen.writeNumberField("accuracy", value.accuracy);

		}
	}
}
