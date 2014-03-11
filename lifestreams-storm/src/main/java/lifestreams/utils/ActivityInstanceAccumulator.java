package lifestreams.utils;

import java.util.List;

import lifestreams.models.StreamRecord;
import lifestreams.models.data.ActivityInstance;
import lifestreams.models.data.IMobilityData;
import lifestreams.models.data.ActivityInstance.TrackPoint;

import com.bbn.openmap.geo.Geo;

public class ActivityInstanceAccumulator {
	ActivityInstance instance = new ActivityInstance();
	KalmanLatLong filter = new KalmanLatLong((float) 1); // Q meter per second = 2
	// return if this accumulator has been init (i.e. contains any data points)
	public boolean isInitialized(){
		return instance.getStartTime() != null;
	}
	public void addDataPoint(StreamRecord<IMobilityData> point) {
		if (!point.d().getMode().isActive())
			throw new RuntimeException(	"The given mobility state is not a active state");
		if (instance.getStartTime() == null) {
			instance.setStartTime(point.getTimestamp());
		}
		instance.setEndTime(point.getTimestamp());
		if (instance.getStartTime().isAfter(instance.getEndTime()))
			throw new RuntimeException("We are going back in time!?");
		// add the type of activity
		instance.getTypes().add(point.d().getMode());

		if (point.getLocation() != null && point.getLocation().getAccuracy() < 100) {
			Geo geo = point.getLocation().getCoordinates();
			// apply kalman latlng filter to the location point
			filter.Process(geo.getLatitude(), geo.getLongitude(), 
							(float) point.getLocation().getAccuracy(), 
							point.getTimestamp().getMillis());
			// get the smoothed location fromthe filter
			instance.getTrackPoints().add(
					new ActivityInstance.TrackPoint(filter.get_lat(), filter.get_lng(), point.getTimestamp())
			);
		}

	}

	public ActivityInstance getInstance() {
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
		instance.setDistance(distance);
		return instance;
	}
}
