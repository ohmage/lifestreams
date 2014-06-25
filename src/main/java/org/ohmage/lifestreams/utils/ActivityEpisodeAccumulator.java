package org.ohmage.lifestreams.utils;

import com.javadocmd.simplelatlng.LatLng;
import com.javadocmd.simplelatlng.LatLngTool;
import com.javadocmd.simplelatlng.util.LengthUnit;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.ohmage.lifestreams.models.StreamRecord;
import org.ohmage.lifestreams.models.data.ActivityEpisode;
import org.ohmage.lifestreams.models.data.ActivityEpisode.TrackPoint;
import org.ohmage.lifestreams.models.data.IMobilityData;
import org.slf4j.Logger;

import java.util.List;

public class ActivityEpisodeAccumulator {
    final static private Logger logger = org.slf4j.LoggerFactory.getLogger(ActivityEpisodeAccumulator.class);
	private final ActivityEpisode instance = new ActivityEpisode();
	private final KalmanLatLong filter = new KalmanLatLong((float) 4); // Q meter per second = 2

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

        // only take those points with geo location and good accuracy
		if (point.getLocation() != null && point.getLocation().getAccuracy() < 100) {
			LatLng geo = point.getLocation().getCoordinates();
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
			LatLng curLocation = new LatLng(points.get(0).getLat(), points.get(0).getLng());
            DateTime curTime = points.get(0).getTime();
			for (TrackPoint point : points) {
				LatLng nextLocation = new LatLng(point.getLat(), point.getLng());
                DateTime nextTime = point.getTime();
                double displacementInMiles = LatLngTool.distance(curLocation, nextLocation, LengthUnit.MILE);
                double duration = new Duration(curTime, nextTime).getStandardSeconds() / 3600.0;
                double speedInMilesInHours = displacementInMiles / duration;
                // don't count the track points which move too fast to be true...
                if(speedInMilesInHours < 7.0) {
                    distance += displacementInMiles;
                }
                curTime = nextTime;
				curLocation = nextLocation;
			}
		}
		instance.setDistanceInMiles(distance);
		return instance;
	}
}
