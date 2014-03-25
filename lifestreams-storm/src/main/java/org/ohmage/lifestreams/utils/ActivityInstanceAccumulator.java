package org.ohmage.lifestreams.utils;

import java.util.List;

import org.ohmage.lifestreams.models.StreamRecord;
import org.ohmage.lifestreams.models.data.ActivityEpisode;
import org.ohmage.lifestreams.models.data.IMobilityData;
import org.ohmage.lifestreams.models.data.ActivityEpisode.TrackPoint;

import com.bbn.openmap.geo.Geo;

public class ActivityInstanceAccumulator {
	ActivityEpisode instance = new ActivityEpisode();
	KalmanLatLong filter = new KalmanLatLong((float) 1); // Q meter per second = 2
	// return if this accumulator has been init (i.e. contains any data points)
	public boolean isInitialized(){
		return instance.getStartTime() != null;
	}
	public void addDataPoint(StreamRecord<IMobilityData> point) {
		if (!point.d().getMode().isActive())
			throw new RuntimeException(	"The given mobility state is not a active state");
		if (!isInitialized()) {
			instance.setStartTime(point.getTimestamp());
			instance.setEndTime(point.getTimestamp());
		}else if(point.getTimestamp().isAfter(instance.getEndTime())){
			instance.setEndTime(point.getTimestamp());
		}else{
			return;
		}
		
		// add the type of activity
		instance.getTypes().add(point.d().getMode());

		if (point.getLocation() != null && point.getLocation().getAccuracy() < 100) {
			// only take those points with geo location and good accuracy
			Geo geo = point.getLocation().getCoordinates();
			// apply kalman latlng filter to the location point
			filter.Process(geo.getLatitude(), geo.getLongitude(), 
							(float) point.getLocation().getAccuracy(), 
							point.getTimestamp().getMillis());
			// get the smoothed location fromthe filter
			instance.getTrackPoints().add(
					new ActivityEpisode.TrackPoint(filter.get_lat(), filter.get_lng(), point.getTimestamp())
			);
			
		}

	}

	public ActivityEpisode getInstance() {
		List<TrackPoint> points = instance.getTrackPoints();
		double distance = 0;
		// moving avg with 5 mins time window
		for (TrackPoint point : points) {
			double accumulatedLat = 0 ;
			double accumulatedLng = 0 ;
			int numPoints = 0;
			for (TrackPoint otherPoint : points) {
				if(Math.abs(point.getTime().getMillis() - otherPoint.getTime().getMillis()) < 2.5 * 60 * 1000){
					accumulatedLat += point.getLat();
					accumulatedLng += point.getLng();
					numPoints ++;
				}
				
			}
			point.setLat(accumulatedLat/numPoints);
			point.setLng(accumulatedLng/numPoints);
		}
		// compute the distance in mile
		if(points.size() > 0){
			Geo curLocation = new Geo(points.get(0).getLat(), points.get(0).getLng(), true);
			for (TrackPoint point : points) {
				Geo nextLocation = new Geo(point.getLat(), point.getLng(), true);
				distance += UnitConversion.NMToMile(Geo.distanceNM(curLocation, nextLocation));
				curLocation = nextLocation;
			}
		}
		instance.setDistanceInMiles(distance);
		
		return instance;
	}
}
