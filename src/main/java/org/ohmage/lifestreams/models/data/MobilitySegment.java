package org.ohmage.lifestreams.models.data;

import java.util.List;
import java.util.Map.Entry;

import org.joda.time.DateTime;
import org.ohmage.lifestreams.models.GeoLocation;
import org.ohmage.lifestreams.models.MobilityState;
import org.ohmage.lifestreams.models.StreamRecord;

import com.javadocmd.simplelatlng.LatLng;

import fr.dudie.nominatim.model.Address;

public class MobilitySegment{
	
	public enum State {
		Place, Moves
	}
	static public class MoveSegment{
		final DateTime begin;
		final DateTime end;
		final LatLng from;
		final LatLng dest;
		final List<StreamRecord> trackpoints;
		
		public MoveSegment(DateTime begin, DateTime end, LatLng from,
				LatLng dest, List<StreamRecord> trackpoints) {
			super();
			this.begin = begin;
			this.end = end;
			this.from = from;
			this.dest = dest;
			this.trackpoints = trackpoints;
		}
	}
	static public class PlaceSegment {
		public PlaceSegment(DateTime begin, DateTime end, LatLng coordinates, Address address) {
			super();
			this.address = address;
			this.begin = begin;
			this.end = end;
			this.coordinates = coordinates;
		}
		public Address getAddress() {
			return address;
		}
		public DateTime getBegin() {
			return begin;
		}
		public DateTime getEnd() {
			return end;
		}
		final Address address;
		final DateTime begin;
		final DateTime end;
		final LatLng coordinates;
	}
	

	private MoveSegment moveSegment;
	private PlaceSegment placeSegment;
	
	public Long getTimeSpanInSecs(){
		if(moveSegment != null){
			return moveSegment.end.getMillis() - moveSegment.begin.getMillis() / 1000; 
		}else{
			return placeSegment.end.getMillis() - placeSegment.begin.getMillis() / 1000; 
		}
	}
	public MoveSegment getMoveSegment() {
		return moveSegment;
	}
	public PlaceSegment getPlaceSegment() {
		return placeSegment;
	}
	public State getSegmentType(){
		return moveSegment != null ? State.Moves : State.Place;
	}
	public MobilitySegment(MoveSegment moveSegment){
		this.moveSegment = moveSegment;
	}
	public MobilitySegment(PlaceSegment placeSegment){
		this.placeSegment = placeSegment;
	}
}
