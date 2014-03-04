package lifestreams.bolt;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.bbn.openmap.geo.ConvexHull;
import com.bbn.openmap.geo.Geo;

import lifestreams.model.StreamRecord;
import lifestreams.model.data.GeoDiameterData;
import lifestreams.utils.TimeWindow;
import backtype.storm.topology.BasicOutputCollector;

/**
 * @author changun This task compute the geodiameter in a time window by
 *         updating a convex hull every 10 data points
 */
@SuppressWarnings("rawtypes")
public class GeoDiameterTask extends SimpleTask {

	private static final long serialVersionUID = 5133741379346160935L;

	private static final int UNPROCESSED_POINTS_BUFFER_SIZE = 10;

	List<StreamRecord> unprocessedPoints = new ArrayList<StreamRecord>(
			UNPROCESSED_POINTS_BUFFER_SIZE);
	List<StreamRecord> currentConexHull = new ArrayList<StreamRecord>();

	private void updateCovexHull() {
		if (unprocessedPoints.size() == 0) {
			// do nothing if no unprocessed point
			return;
		}

		// combine the unprocessed points with the points on the current convex
		// hull
		unprocessedPoints.addAll(currentConexHull);
		// prepare an geolocation array for computing convex hull
		List<Geo> geoPoints = new LinkedList<Geo>();
		for (StreamRecord point : unprocessedPoints) {
			geoPoints.add(point.getLocation().getCoordinates());
		}

		// get the combined convex hull
		Geo[] hull = ConvexHull
				.hull(geoPoints.toArray(new Geo[geoPoints.size()]));

		// replace the current old convex hull with new one
		// notice: we record a convex hull by storing the vertexes of the hull
		currentConexHull.clear();
		for (Geo location : hull) {
			// get the geolocation of each vertex
			for (StreamRecord point : unprocessedPoints) {
				// for each point, check if it has the same location as the
				// vertex
				if (point.getLocation().getCoordinates() == location) {
					currentConexHull.add(point);
					break;
				}
			}
		}
		// clear the unprocessed points buffer
		unprocessedPoints.clear();
	}

	@Override
	public void executeDataPoint(StreamRecord dp, TimeWindow window,
			BasicOutputCollector collector) {
		if (dp.getLocation() == null)
			return;
		// compute the current convex hull every 10 data points
		if (unprocessedPoints.size() >= UNPROCESSED_POINTS_BUFFER_SIZE) {
			updateCovexHull();
		} else {
			unprocessedPoints.add(dp);
		}
	}

	private StreamRecord<GeoDiameterData> computeGeoDistance(TimeWindow window) {
		// first add all the unprocessed data points to generate the new convex
		// hull
		updateCovexHull();
		List<StreamRecord> hull_points = this.currentConexHull;
		if (hull_points.size() < 2) {
			// return nothing if we don't have a convex hull...
			return null;
		}
		double longestDistanceInHull = 0.0;
		StreamRecord earlierPointOnDiameter = null;
		StreamRecord laterPointOnDiameter = null;
		// compute the pairwise distance between the vertexes and get the
		// longest distance
		for (int i = 0; i < hull_points.size() - 1; i++) {
			for (int j = i + 1; j < hull_points.size(); j++) {
				Geo x = hull_points.get(i).getLocation().getCoordinates();
				Geo y = hull_points.get(j).getLocation().getCoordinates();
				// compute the diameter in miles
				double distance = x.distanceNM(y);
				if (longestDistanceInHull < distance) {
					// record the points that have the longest distance so far
					longestDistanceInHull = distance;
					earlierPointOnDiameter = hull_points.get(i);
					laterPointOnDiameter = hull_points.get(j);
				}
			}
		}
		// create the data point
		GeoDiameterData data = new GeoDiameterData(window, this)
				.setDiameter(longestDistanceInHull)
				.setEarlierPointOnDiameter(earlierPointOnDiameter.getLocation())
				.setLaterPointOnDiameter(laterPointOnDiameter.getLocation());

		return new StreamRecord<GeoDiameterData>(getUser(),
				window.getLastInstant(), data);
	}

	@Override
	public void finishWindow(TimeWindow window, BasicOutputCollector collector) {
		StreamRecord<GeoDiameterData> rec = computeGeoDistance(window);
		if (rec != null) {
			this.emit(rec, collector);
		}
		// clear convex hull points
		currentConexHull.clear();

	}

	@Override
	public void snapshotWindow(TimeWindow window, BasicOutputCollector collector) {

		StreamRecord<GeoDiameterData> rec = computeGeoDistance(window);
		if (rec != null) {
			this.emitSnapshot(rec, collector);
		}

	}

}
