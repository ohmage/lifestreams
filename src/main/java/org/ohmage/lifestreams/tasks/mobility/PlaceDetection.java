package org.ohmage.lifestreams.tasks.mobility;

import com.javadocmd.simplelatlng.LatLng;
import com.javadocmd.simplelatlng.LatLngTool;
import com.javadocmd.simplelatlng.util.LengthUnit;
import fr.dudie.nominatim.model.Address;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.ohmage.lifestreams.models.GeoLocation;
import org.ohmage.lifestreams.models.StreamRecord;
import org.ohmage.lifestreams.models.data.MobilityData;
import org.ohmage.lifestreams.models.data.MobilitySegment;
import org.ohmage.lifestreams.models.data.MobilitySegment.MoveSegment;
import org.ohmage.lifestreams.models.data.MobilitySegment.PlaceSegment;
import org.ohmage.lifestreams.models.data.MobilitySegment.State;
import org.ohmage.lifestreams.tasks.SimpleTask;
import org.ohmage.lifestreams.utils.DivideInterval;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * @author changun
 *
 */
public class PlaceDetection extends SimpleTask<MobilityData> {
	private static final int TEN_MINS = 60 * 10 * 1000;

	// OSM API requester's email
	private final String nominatimRequesterEmail;
	// the state of last data point of which we just determin its state
	private State prevState = State.Place;
	// the segment we are currently aggregating
	private LinkedList<StreamRecord> curSegment = new LinkedList<StreamRecord>();
	// data point buffer
    private LinkedList<StreamRecord<MobilityData>> data = new LinkedList<StreamRecord<MobilityData>>();
	// client for querying OSM address
	private transient CachedOpenStreetMapClient OSMClient;
	
	@Override
	public void init() {
		OSMClient = new CachedOpenStreetMapClient(nominatimRequesterEmail, this.getMapFactory());
	}
	@Override
	public void recover() {
		init();
	}

	/**
	 * Create and output a place segment with the given data points
	 * 
	 * @param placePoints
	 */
	private void outputPlaceSegment(LinkedList<StreamRecord> placePoints) {
		// compute the median lat/lng of all points in the place
		DescriptiveStatistics lat = new DescriptiveStatistics();
		DescriptiveStatistics lng = new DescriptiveStatistics();
		for (StreamRecord rec : placePoints) {
			lat.addValue(rec.getLocation().getCoordinates().getLatitude());
			lng.addValue(rec.getLocation().getCoordinates().getLongitude());
		}
		double mLat = lat.getPercentile(50.0);
		double mLng = lng.getPercentile(50.0);

		// query OpenStreetMap for context info
		Address address;
		try {
			address = OSMClient.getAddress(mLat, mLng);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		if (address.getAddressElements() == null) {
			getLogger().error("Cannot find place info with coordinates {},{}", mLat, mLng);
		}

		LatLng location = new LatLng(mLat, mLng);
		Interval wholeInterval = new Interval(placePoints.getFirst().getTimestamp(), 
											  placePoints.getLast().getTimestamp());
		// divide the whole interval by the day boundaries and emit one segment for each sub-interval
		for(Interval interval: DivideInterval.byDay(wholeInterval)){
			PlaceSegment output = new PlaceSegment(interval.getStart(), 
												   interval.getEnd(), 
												   location, address);
			GeoLocation geolocation = new GeoLocation(interval.getEnd(), location, -1, "PlaceDetection");
			this.createRecord().setData(new MobilitySegment(output))
							   .setTimestamp(interval.getEnd())
							   .setLocation(geolocation)
							   .emit();
		}
	
	}
	/**
	 * Create and output a Move segment with the given data points
	 * 
	 * @param placePoints
	 */
	private void outputMoveSegment(LinkedList<StreamRecord> movePoints) {
		DateTime start = movePoints.getFirst().getTimestamp();
		DateTime end = movePoints.getLast().getTimestamp();
		LatLng origin = movePoints.getFirst().getLocation().getCoordinates();
		LatLng destination = movePoints.getLast().getLocation().getCoordinates();
		Interval wholeInterval = new Interval(start, end);
		// divide the whole interval by the day boundaries and emit one segment for each sub-interval
		for(Interval interval: DivideInterval.byDay(wholeInterval)){
			MoveSegment output = new MoveSegment(start, end, origin, destination, movePoints);
			this.createRecord().setData(new MobilitySegment(output))
							   .setTimestamp(interval.getEnd())
							   .emit();
		}
	}
	
	private void outputSegment(LinkedList<StreamRecord> segment, State state){
		if(state == State.Place){
			outputPlaceSegment(segment);
		}else{
			outputMoveSegment(segment);
		}
	}
	/**
	 * Update the Move/Place segment we are currently accumulating. If the given state differs 
	 * from the segment state, output the old segment and create a new one.
	 * 
	 * @param rec new record to be aggregate
	 * @param state the new state (Moving/Place)
	 */
	private void accumulateNewDataPoint(StreamRecord<MobilityData> newRec, State newState){
		boolean prevSegmentExists = curSegment.size() > 0;
 
		boolean hasOutput = false;
		if(prevSegmentExists){
			boolean stateChanged = prevState != newState;
			long gapBetweenRecords = new Interval(curSegment.getLast().getTimestamp(), 
									newRec.getTimestamp()).toDurationMillis();
			boolean gapTooLarge = gapBetweenRecords > (20 * 60 * 1000L);
			boolean prevSegmentTooLong =  new Interval(curSegment.getFirst().getTimestamp(), 
													  curSegment.getLast().getTimestamp()).toDuration().getStandardHours() > 3;
			if(stateChanged || gapTooLarge){
				// output the old segment, if the state changed
				outputSegment(curSegment, prevState);
				curSegment.clear();
				hasOutput = true;
			}else if(prevSegmentTooLong && curSegment.size() > 20){
				// keep the latest 10 records
				List<StreamRecord> subList = curSegment.subList(0 , curSegment.size()-10);
				LinkedList<StreamRecord> outputSeg = new LinkedList<StreamRecord>(subList);
				outputSegment(outputSeg, prevState);
				// remove the records that have been output
				subList.clear();
				hasOutput = true;
			}
		}
		
		curSegment.add(newRec);
		// update prevState (this is important!)
		prevState = newState;
		if(hasOutput){
			this.checkpoint();
		}
	}
	private Long getHeadMillis() {
		return data.getFirst().getLocation().getTimestamp().getMillis();
	}

	@Override	
	public void executeDataPoint(StreamRecord<MobilityData> newRec) {
		if(newRec.getLocation() == null){
			// don't include the data point if it does not have location info
			return;
		}else if(data.size() > 0){
			 // skip out-of-order records
			DateTime lastTime = data.getLast().getLocation().getTimestamp();
			DateTime newTime = newRec.getLocation().getTimestamp();
			if(lastTime.isAfter(newTime)){
				 return;
			}
		}
		data.add(newRec);
		long curMillis = newRec.getLocation().getTimestamp().getMillis();

		// determine the head record's Place/Move state if we have received
		// all the records within the next 10 minutes from its timestamp
		for (long headMillis = getHeadMillis(); curMillis - headMillis > TEN_MINS; headMillis = getHeadMillis()) {
			// get head record
			StreamRecord<MobilityData> headRec = data.removeFirst();

			// compute the max displacement from the head data point to its subsequent data points within the next 10 minutes
			LatLng firstLatLng = headRec.getLocation().getCoordinates();
			Iterator<StreamRecord<MobilityData>> iter = data.iterator();
			double maxDisplacement = -1;
			while (iter.hasNext()) {
				StreamRecord<MobilityData> next = iter.next();
				long nextMillis = next.getLocation().getTimestamp().getMillis();
				// check if the next rec is within the next 10 mins 
				if ((nextMillis - headMillis) > TEN_MINS) {
					break;
				}
				LatLng nextLatLng = next.getLocation().getCoordinates();

				double maxDiff = Math.max(Math.abs(nextLatLng.getLatitude() - firstLatLng.getLatitude()) , 
										  Math.abs(nextLatLng.getLongitude() - firstLatLng.getLongitude())); 
				double displacement;
				if(maxDiff > 0.0001){
					displacement = LatLngTool.distance(firstLatLng, nextLatLng, LengthUnit.METER);
				}else{
					displacement = 0;
				}
				maxDisplacement = Math.max(displacement, maxDisplacement);
			}
			// determine state based on 1) previous state 2) Mobility and 3)
			// displacements
			State curState;
			if (maxDisplacement >= 0) {
				if (headRec.getData().getMode().isActive() && maxDisplacement > 100.0){
					// if it walking or running
					curState = State.Moves;
				} else if (maxDisplacement > 2000.0) {
					// probably transportation
					curState = State.Moves;
				} else if (maxDisplacement < 50) {
					// must be still in a place
					curState = State.Place;
				}else{
					// use the prev state is no idea
					curState = prevState;
				}
			}else{
				// use the prev state if no data point in the next 10 mins
				curState = prevState;
			}
			
			accumulateNewDataPoint(headRec, curState);
		}
	}
	@SuppressWarnings("SameParameterValue")
    private PlaceDetection(String nominatimRequesterEmail) {
		super();
		this.nominatimRequesterEmail = nominatimRequesterEmail;
	}
	public PlaceDetection() {
		this("your@email.com");
	}
}
