package lifestreams.bolt;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import lifestreams.model.DataPoint;
import lifestreams.model.OhmageUser;

import org.joda.time.Duration;
import org.joda.time.Period;
import org.joda.time.base.BaseSingleFieldPeriod;

import com.bbn.openmap.geo.ConvexHull;
import com.bbn.openmap.geo.Geo;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import backtype.storm.topology.BasicOutputCollector;

public class GeoDistanceBolt extends BasicLifestreamsBolt {


	

	public GeoDistanceBolt(BaseSingleFieldPeriod period) {
		super(period);
		// TODO Auto-generated constructor stub
	}

	@Override
	protected void executeBatch(OhmageUser user, List<DataPoint> data, BasicOutputCollector collector) {
		// an array of Geo points
		List<Geo> GeoPoints = new ArrayList<Geo>(data.size()); 
		for(int i=0; i<data.size(); i++){
			// add the geo location to the geo points set when available
			if( data.get(i).getMetadata().get("location")!=null){
				Double lat = data.get(i).getMetadata().get("location").get("latitude").asDouble();
				Double lng = data.get(i).getMetadata().get("location").get("longitude").asDouble();
				// covert data to Geo point objects
				Geo g = new Geo(lat,lng, true);
				GeoPoints.add(g);
			}
		}
		if(GeoPoints.size() > 0){
			// get the convex hull of the points
			Geo[] hull = ConvexHull.hull(GeoPoints.toArray(new Geo[GeoPoints.size()]));
			
			// compute the longest distance between the vertex on the convex hull
			double longestDistanceInHull = 0.0;
			for(int i=0; i<hull.length-1; i++){
				for(int j=i+1; j<hull.length; j++){
					double distance = hull[i].distanceKM(hull[j]);
					longestDistanceInHull = Math.max(longestDistanceInHull, distance);
				}
			}
			
			// emit the geodistance
			ObjectMapper mapper = new ObjectMapper();
			ObjectNode dataTable = mapper.createObjectNode();
			dataTable.put("daily_geodistance", longestDistanceInHull);
			DataPoint dp = new DataPoint(user,data.get(0).getTimestamp(), dataTable, dataTable);
			// print debug message
			System.out.printf("Thread %d GeoDiameter %s(%s) %f\n", Thread.currentThread().getId(), 
					user.getUsername(), dp.getTimestamp(), longestDistanceInHull);
			emit(dp, collector);
		}
		
	}
	
}
