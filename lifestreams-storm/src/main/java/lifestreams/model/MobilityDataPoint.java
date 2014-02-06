package lifestreams.model;

import lifestreams.bolt.MobilityState;

import org.joda.time.DateTime;

import com.bbn.openmap.geo.Geo;
import com.fasterxml.jackson.databind.node.ObjectNode;
// TODO: implement getter methods for the other fields
public class MobilityDataPoint extends DataPoint {
	
	public MobilityDataPoint(OhmageUser user, DateTime timestamp,
			ObjectNode data, ObjectNode metadata) {
		super(user, timestamp, data, metadata);
	}
	public MobilityState getState(){
		return MobilityState.valueOf(this.getData().get("mode").asText().toUpperCase());
	}
	
	public void setState(MobilityState state){
		this.getData().put("mode", state.toString());
	}
	public Geo getLocation(){
		if(this.getMetadata().get("location")!=null){
			Double lat = this.getMetadata().get("location").get("latitude").asDouble();
			Double lng = this.getMetadata().get("location").get("longitude").asDouble();
			// covert data to Geo point objects
			return new Geo(lat,lng, true);
		}
		return null;
	}
	
	public boolean hasLocation(){
		return this.getMetadata().get("location")!=null;
	}

}
