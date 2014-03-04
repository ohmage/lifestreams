package lifestreams.utils;

import java.util.List;

import com.bbn.openmap.geo.Geo;

import lifestreams.model.data.ActivityInstance;
import lifestreams.model.data.IMobilityData;
import lifestreams.model.data.ActivityInstance.TrackPoint;
import lifestreams.model.StreamRecord;

public class ActivityInstanceAccumulator {
	ActivityInstance instance = new ActivityInstance();
	KalmanLatLong filter = new KalmanLatLong(2); // Q meter per second = 2

	public void addDataPoint(StreamRecord<IMobilityData> point) {
		if (!point.d().getMode().isActive())
			throw new RuntimeException(
					"The given mobility state is not a active state");
		if (instance.getStartTime() == null) {
			instance.setStartTime(point.getTimestamp());
		}
		instance.setEndTime(point.getTimestamp());
		if (instance.getStartTime().isAfter(instance.getEndTime()))
			throw new RuntimeException("We are going back in time!?");
		// add the type of activity
		instance.getTypes().add(point.d().getMode());

		if (point.getLocation() != null) {
			Geo geo = point.getLocation().getCoordinates();
			// apply kalman latlng filter to the location point
			filter.SetState(geo.getLatitude(), geo.getLongitude(),
					(float) point.getLocation().getAccuracy(), point
							.getTimestamp().getMillis());
			// get the smoothed location fromthe filter
			instance.getTrackPoints().add(
					new ActivityInstance.TrackPoint(filter.get_lat(), filter
							.get_lng(), point.getTimestamp()));
		}

	}

	public ActivityInstance getInstance() {
		List<TrackPoint> points = instance.getTrackPoints();
		// compute the distance in mile
		Geo curLocation = new Geo(points.get(0).getLat(), points.get(0)
				.getLng(), true);
		double distance = 0;
		for (TrackPoint point : points) {
			Geo nextLocation = new Geo(point.getLat(), point.getLng(), true);
			distance += Geo.distanceNM(curLocation, nextLocation);
		}
		instance.setDistance(distance);
		return instance;
	}
}
