package org.ohmage.lifestreams.utils;

import java.util.ArrayList;
import java.util.List;

import org.joda.time.DateTime;
import org.ohmage.lifestreams.models.GeoLocation;
import org.ohmage.lifestreams.models.StreamRecord;
import org.ohmage.lifestreams.models.data.ActivityEpisode;
import org.ohmage.lifestreams.models.data.IMobilityData;
import org.ohmage.lifestreams.models.data.ActivityEpisode.TrackPoint;

import com.bbn.openmap.geo.Geo;

public class ActivityEpisodeAccumulator {
	ActivityEpisode instance = new ActivityEpisode();
	KalmanLatLong filter = new KalmanLatLong((float) 4); // Q meter per second = 2
	ArrayList<GeoLocation> rawGeoLocations = new ArrayList<GeoLocation>();
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
		rawGeoLocations.add(point.getLocation());
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
	public DateTime getEndTime(){
		return this.instance.getEndTime();
	}
	public ActivityEpisode getEpisode() {
		List<TrackPoint> points = instance.getTrackPoints();
		double distance = 0;
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
