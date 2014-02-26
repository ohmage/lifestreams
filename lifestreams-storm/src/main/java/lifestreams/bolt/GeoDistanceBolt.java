package lifestreams.bolt;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import lifestreams.model.GeoDiameterDataPoint;
import lifestreams.model.IDataPoint;
import lifestreams.model.MobilityDataPoint;

import org.ohmage.models.OhmageUser;
import org.ohmage.models.OhmageUser.OhmageAuthenticationError;

import lifestreams.utils.TimeWindow;

import org.joda.time.Duration;
import org.joda.time.Period;
import org.joda.time.base.BaseSingleFieldPeriod;

import state.UserState;

import com.bbn.openmap.geo.ConvexHull;
import com.bbn.openmap.geo.Geo;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;

public class GeoDistanceBolt extends LifestreamsBolt {

	public GeoDistanceBolt(BaseSingleFieldPeriod period) {
		super(period);
		// TODO Auto-generated constructor stub
	}

	@Override
	protected void newUser(OhmageUser user, UserState state){
		super.newUser(user, state);
		List<MobilityDataPoint> points = new LinkedList<MobilityDataPoint>(); 
		state.put("UNPROCESSED_POINTS", points);
		List<MobilityDataPoint> convex_points = new LinkedList<MobilityDataPoint>(); 
		state.put("CONVEX_POINTS", points);
	}
	private void updateCovexHull(UserState state){
		List<MobilityDataPoint> unprocessedPoints = (List<MobilityDataPoint>) state.get("UNPROCESSED_POINTS");
		if(unprocessedPoints.size() == 0)
			return;
		
		// combine the unprocessed points with the points in the prev convex hull
		unprocessedPoints.addAll((List<MobilityDataPoint>) state.get("CONVEX_POINTS"));
		// create an array to compute convex hull
		List<Geo> geoPoints = new LinkedList<Geo>();
		for(MobilityDataPoint point: unprocessedPoints){
			geoPoints.add(point.getLocation().getCoordinates());
		}
		
		// get the combined convex hull
		Geo[] hull = ConvexHull.hull(geoPoints.toArray(new Geo[geoPoints.size()]));
		
		// get those points that are on the convex hull
		List<MobilityDataPoint> convexPoints = new LinkedList<MobilityDataPoint>();
		for(Geo location: hull){
			for(MobilityDataPoint point: unprocessedPoints){
				if(point.getLocation().getCoordinates() == location){
					convexPoints.add(point);
					break;
				}
			}
		}
		// store these points
		state.put("CONVEX_POINTS", convexPoints);
		// clear the unprocessed points buffer
		unprocessedPoints.clear();
	}
	@Override
	protected boolean executeDataPoint(OhmageUser user, IDataPoint dp,
			UserState state, TimeWindow window, BasicOutputCollector collector) {

			MobilityDataPoint mdp = (MobilityDataPoint)dp;
			if(mdp.getLocation()==null)
				return false;
			List<MobilityDataPoint> unprocessedPoints = (List<MobilityDataPoint>) state.get("UNPROCESSED_POINTS");
			// compute the current convex hull every 10 data points
			if(unprocessedPoints.size() >= 10){
				updateCovexHull(state);
			}
			else{
				unprocessedPoints.add(mdp);
			}
			return false;
	}

	@Override
	protected void finishWindow(OhmageUser user, UserState state,
			TimeWindow window, BasicOutputCollector collector) {
		GeoDiameterDataPoint dp = computeGeoDistance(user, state, window);
		this.emit(dp, collector);

		// clear convex hull points
		List<MobilityDataPoint> hull_points = (List<MobilityDataPoint>) state.get("CONVEX_POINTS");
		hull_points.clear();

	}

	@Override
	protected void snapshotWindow(OhmageUser user, UserState state, TimeWindow window, 
			BasicOutputCollector collector) {
		
		GeoDiameterDataPoint dp = computeGeoDistance(user, state, window);
		this.emitSnapshot(dp, collector);
	}
	
	private GeoDiameterDataPoint computeGeoDistance(OhmageUser user, UserState state, TimeWindow window){
		updateCovexHull(state);
		List<MobilityDataPoint> hull_points = (List<MobilityDataPoint>) state.get("CONVEX_POINTS");
		double longestDistanceInHull = 0.0;
		MobilityDataPoint earlierPointOnDiameter = null;
		MobilityDataPoint laterPointOnDiameter = null;
		// compute the longest distance between the vertex on the convex hull
		for(int i=0; i<hull_points.size()-1; i++){
			for(int j=i+1; j<hull_points.size(); j++){
				Geo x = hull_points.get(i).getLocation().getCoordinates();
				Geo y = hull_points.get(j).getLocation().getCoordinates();
				double distance = x.distanceKM(y);
				if(longestDistanceInHull < distance){
					longestDistanceInHull = distance;
					earlierPointOnDiameter = hull_points.get(i);
					laterPointOnDiameter = hull_points.get(j);
				}
			}
		}
		return new GeoDiameterDataPoint(user, window, this)
					.setDiameterInKM(longestDistanceInHull)
					.setEalierPointOnDiameter(earlierPointOnDiameter)
					.setLaterPointOnDiameter(laterPointOnDiameter);
	}
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		super.prepare(stormConf, context);

	}
}
