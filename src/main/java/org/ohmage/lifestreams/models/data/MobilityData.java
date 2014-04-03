package org.ohmage.lifestreams.models.data;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.ohmage.lifestreams.models.MobilityState;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class MobilityData implements IMobilityData {
	private MobilityState mode;
	private Map<WiFi, Double> wifis;
	private double speed;
	public MobilityData() {

	}

	public Map<WiFi, Double> getWifis() {
		return wifis;
	}
	@JsonProperty("wifi_data")
	@JsonDeserialize(using = WiFiDataDeserializer.class)
	public void setWifis(Map<WiFi, Double> wifis) {
		this.wifis = wifis;
	}
	
	@Override
	public MobilityState getMode() {
		return mode;
	}
	public String toString(){
		return String.format("Mode:%s", this.getMode().toString());
	}
	@JsonDeserialize(using = MobilityStateDeserializer.class)
	public void setMode(MobilityState state) {
		this.mode = state;
	}

	public double getSpeed() {
		return speed;
	}

	public void setSpeed(double speed) {
		this.speed = speed;
	}

	public static class MobilityStateDeserializer extends
			JsonDeserializer<MobilityState> {
		@Override
		public MobilityState deserialize(JsonParser jp,
				DeserializationContext ctxt) throws IOException,
				JsonProcessingException {
			String mode = jp.getText();
			return MobilityState.valueOf(mode.toUpperCase());
		}
		

	}
	
	public static class WiFiDataDeserializer extends JsonDeserializer<Map<WiFi, Double>> {
		@Override
		public Map<WiFi, Double> deserialize(JsonParser jp,
			DeserializationContext ctxt) throws IOException,
			JsonProcessingException {
			ObjectNode dataNode = jp.readValueAs(ObjectNode.class);
			if(!dataNode.has("scan")){
				return null;
			}
	    	Iterator<JsonNode> iter = dataNode.get("scan").iterator();
	    	Map<WiFi, Double> wifis = new HashMap<WiFi, Double>();
	    	while(iter.hasNext()){
	    		JsonNode scan = iter.next();
	    		wifis .put(new WiFi(scan.get("ssid").asText()), scan.get("strength").asDouble());
	    	}
	    	return wifis;
		}
	}
}