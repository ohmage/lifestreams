package org.ohmage.lifestreams.tasks;

import java.util.ArrayList;
import java.util.List;

import org.ohmage.lifestreams.models.GeoLocation;
import org.ohmage.lifestreams.models.StreamRecord;
import org.ohmage.lifestreams.models.data.GeoDiameterData;
import org.springframework.stereotype.Component;

import com.javadocmd.simplelatlng.util.LengthUnit;

/**
 * @author changun This task compute the geodiameter from a set of records (with
 *         geo-location information) by updating a convex hull every 10 records
 */
@SuppressWarnings("rawtypes")
@Component
public class GeoDiameterTask extends SimpleTimeWindowTask {

	private static final long serialVersionUID = 5133741379346160935L;

	private static final int POINTS_BUFFER_SIZE = 10;

	List<StreamRecord> pointBuffer = new ArrayList<StreamRecord>(
			POINTS_BUFFER_SIZE);
	List<StreamRecord> currentConvexHull = new ArrayList<StreamRecord>();

	private void updateCovexHull() {
		if (pointBuffer.size() == 0) {
			// do nothing if no unprocessed point
			return;
		}

		// combine the unprocessed points with the points on the current convex
		// hull
		pointBuffer.addAll(currentConvexHull);
		// get convex hull
		currentConvexHull = ConvexHull.getHull(pointBuffer, 50);
		// clear the unprocessed points buffer
		pointBuffer.clear();
	}

	@Override
	public void executeDataPoint(StreamRecord dp, TimeWindow window) {
		if (dp.getLocation() == null || dp.getLocation().getAccuracy() > 100)
			return;
		// compute the current convex hull every 10 data points
		if (pointBuffer.size() >= POINTS_BUFFER_SIZE) {
			updateCovexHull();
		} else {
			pointBuffer.add(dp);
		}
	}

	private void computeGeoDistance(TimeWindow window) {
		// first add all the unprocessed data points to generate the new convex
		// hull
		updateCovexHull();
		List<StreamRecord> hull_points = this.currentConvexHull;
		if (hull_points.size() < 2) {
			// return nothing if we don't have a convex hull...
			return;
		}
		// take distance between the first two points as the initial distance
		StreamRecord earlierPointOnDiameter = null;
		StreamRecord laterPointOnDiameter = null;
		double longestDistanceInHull = 0;
		// compute the pairwise distance between each pair of vertexes to find
		// the
		// longest distance
		for (int i = 0; i < hull_points.size() - 1; i++) {
			for (int j = i + 1; j < hull_points.size(); j++) {
				GeoLocation x = hull_points.get(i).getLocation();
				GeoLocation y = hull_points.get(j).getLocation();
				// compute the diameter in miles
				double distance = GeoLocation.distance(x, y, LengthUnit.MILE);
				if (longestDistanceInHull <= distance) {
					// record the points that have the longest distance so far
					longestDistanceInHull = distance;
					earlierPointOnDiameter = hull_points.get(i);
					laterPointOnDiameter = hull_points.get(j);
				}
			}
		}
		// create the output data
		GeoDiameterData data = new GeoDiameterData(window, this)
				.setDiameter(longestDistanceInHull)
				.setEarlierPointOnDiameter(earlierPointOnDiameter.getLocation())
				.setLaterPointOnDiameter(laterPointOnDiameter.getLocation());
		// emit data
		this.createRecord().setData(data)
				.setTimestamp(window.getFirstInstant()).emit();

	}

	@Override
	public void finishWindow(TimeWindow window) {
		computeGeoDistance(window);
		// clear convex hull points
		currentConvexHull.clear();
		this.checkpoint(window.getTimeWindowEndTime());
	}
}
