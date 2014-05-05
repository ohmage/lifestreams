package org.ohmage.lifestreams.tasks.mobility;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.math3.ml.clustering.Cluster;
import org.apache.commons.math3.ml.clustering.Clusterable;
import org.apache.commons.math3.ml.clustering.DBSCANClusterer;
import org.apache.commons.math3.ml.distance.DistanceMeasure;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.ohmage.lifestreams.models.GeoLocation;
import org.ohmage.lifestreams.models.StreamRecord;
import org.ohmage.lifestreams.models.data.LeaveReturnHomeTimeData;
import org.ohmage.lifestreams.models.data.MobilityData;
import org.ohmage.lifestreams.models.data.WiFi;
import org.ohmage.lifestreams.tasks.SimpleTimeWindowTask;
import org.ohmage.lifestreams.tasks.TimeWindow;
import org.ohmage.lifestreams.utils.UnitConversion;
import org.springframework.stereotype.Component;

import com.bbn.openmap.geo.Geo;

@Component
public class TimeLeaveReturnHome extends SimpleTimeWindowTask<MobilityData>{

	public TimeLeaveReturnHome() {
		super(Days.ONE);
	}

	List<StreamRecord<MobilityData>> allPoints = new ArrayList<StreamRecord<MobilityData>>();
	final static int MIN_SECONDS_IN_A_PLACE = 7200;
	final static double EPS_IN_MILES = 0.05;
	final static int SLIDING_WINDOW_SIZE_IN_MILLISECS =  15 * 60 * 1000;
	class WiFiGPSDistance implements DistanceMeasure{
		@Override
		public double compute(double[] arg0, double[] arg1) {
			StreamRecord<MobilityData> p0 = allPoints.get((int)arg0[0]);
			StreamRecord<MobilityData> p1 = allPoints.get((int)arg1[0]);
			// first use the WIFI to determine distance
			if(p0.d().getWifis() != null && p1.d().getWifis() != null){
				Set<WiFi> intersection = new HashSet<WiFi>(p0.d().getWifis().keySet());
				intersection.retainAll(p1.d().getWifis().keySet());
				if((double)intersection.size() / (double) p0.d().getWifis().size() > 0.7){
					// if two data points contains over 70% of the same AP, assume they are very close
					return 0.001;
				}
			}
			if(p0.getLocation() == null 
					|| p0.getLocation().getCoordinates() == null 
					|| p1.getLocation() == null 
					|| p1.getLocation().getCoordinates() == null
					){
				// if one of the data points does not have GPS, return MAX distance
				return Double.MAX_VALUE;
			}
			// compute geo distance in miles
			double distanceInMile = UnitConversion.NMToMile(
					p0.getLocation().getCoordinates().distanceNM(p1.getLocation().getCoordinates()));
			// minus the Max(accuracy) / 2
			distanceInMile -= UnitConversion.MeterToMile(
					Math.max(p0.getLocation().getAccuracy(), p1.getLocation().getAccuracy())) / 2 ;
			return (Math.max(0.0, distanceInMile));
		}

	}
	class ClusterableMobilityData implements Clusterable{
		final StreamRecord<MobilityData> data;
		final int index;
		int place = 0;
		int smoothedPlace = 0;
		@Override
		public double[] getPoint() {
			double[] vector = {this.index};
			return vector;
		}
		public DateTime getTimestamp(){
			return data.getTimestamp();
		}
		public ClusterableMobilityData(int index, StreamRecord<MobilityData> data){
			this.data = data;
			this.index = index;
		}
	}
	@Override
	public void executeDataPoint(StreamRecord<MobilityData> dp, TimeWindow window) {
		if((dp.getLocation() != null && dp.getLocation().getAccuracy() < 150) || 
				(dp.getData().getWifis() != null && dp.getData().getWifis().size() > 0)){
			// only include the data that has good GPS, or WiFi
			allPoints.add(dp);
		}
	}

	@Override
	public void finishWindow(TimeWindow window) {
		DateTime timeLeaveHome = null;
		DateTime timeReturnHome = null;
		if(window.getHeuristicMissingDataRate() < 0.5 && allPoints.size() > 0){
			// only generate the measurement for those days we have > 50% of samples,
			
			// make a list of clusterable points
			List<ClusterableMobilityData> cPoints = new ArrayList<ClusterableMobilityData>();
			for(int i=0; i<allPoints.size(); i++){
				cPoints.add(new ClusterableMobilityData(i, allPoints.get(i)));
			}
			// create a distance measure
			WiFiGPSDistance measure = new WiFiGPSDistance();
			// set minPts in a way that only those places the user stays for more than MIN_SECONDS_IN_A_PLACE
			// will be identified as a cluster
			int minPts = (int) (MIN_SECONDS_IN_A_PLACE / window.getMedianSamplingIntervalInSecond());
			// perform DBSCAN
			DBSCANClusterer<ClusterableMobilityData> clusterer = 
					new DBSCANClusterer<ClusterableMobilityData>(EPS_IN_MILES, minPts, measure);
			// it returns a list of cluster, each of which contains the points in that cluster
			List<Cluster<ClusterableMobilityData>> clusters = clusterer.cluster(cPoints);
			
			for(Cluster<ClusterableMobilityData> cluster: clusters){
				// for each cluster (i.e. a place), assign a unique place ID to it.
				int placeID =  clusters.indexOf(cluster) + 1;
				for(ClusterableMobilityData d: cluster.getPoints()){
					d.place = placeID;
				}
			}
			// smooth the place classification results by a sliding window
			for(ClusterableMobilityData point : cPoints){
				int[] placeCount = new int[clusters.size()+1];
				for(ClusterableMobilityData pointInWindow : cPoints){
					if(Math.abs(pointInWindow.getTimestamp().getMillis() - point.getTimestamp().getMillis()) < SLIDING_WINDOW_SIZE_IN_MILLISECS ){
						placeCount[pointInWindow.place] ++;
					}
				}
				// find the places that occurs most frequently in the time window
				int maxCount = 0, maxCountPlace = 0;
				for(int i=0; i<placeCount.length; i++){
					// when tie, the place that point originally belongs to has higher priority
					if(placeCount[i] > maxCount || (placeCount[i] == maxCount && i == point.place)){
						maxCount = placeCount[i];
						maxCountPlace = i;
					}
				}
				point.smoothedPlace = maxCountPlace;
			}
			
			// first place in the time window
			int firstPlace = cPoints.get(0).smoothedPlace;
			// the last place in the time window
			int lastPlace =  cPoints.get(cPoints.size()-1).smoothedPlace;
			
			int i=0;
			if(firstPlace != 0){// if the first place is not a noise point
				for(i=1; i<cPoints.size(); i++){
					if(cPoints.get(i).smoothedPlace != firstPlace ){
						// find the time the user leaving from the home 
						break;
					}
				}
				// if the user ever leave the home
				if(i != cPoints.size()){
					timeLeaveHome = allPoints.get(i-1).getTimestamp();
				}
			}

			// if the user return to the home at the end of the day
			if(firstPlace == lastPlace){
				for(i=cPoints.size()-1; i>=0; i--){
					// find the time the user return the home 
					if(cPoints.get(i).smoothedPlace != lastPlace){
						break;
					}
				}
				if(i != -1){
					timeReturnHome = allPoints.get(i+1).getTimestamp();
				}
			}

			if(timeLeaveHome != null && timeReturnHome != null){
				// compute the amount of time the user is at home
				int timeAtHome  = -1;
				
				// get all the time frames the user is at home
				HashSet<Long> timeFrames = new  HashSet<Long>();
				for(ClusterableMobilityData d: cPoints){
					// the length of each timeframe = MedianSamplingIntervalInSecond
					if(d.smoothedPlace == firstPlace){
						timeFrames.add(d.data.getTimestamp().getMillis() / 1000 / window.getMedianSamplingIntervalInSecond());
					}
				}
				// calculate the total duration of timeframes at home
				timeAtHome = (int) (window.getMedianSamplingIntervalInSecond() * timeFrames.size());
				// scale it up with (1 + miss data rate)
				timeAtHome /= (1 - window.getHeuristicMissingDataRate());
				
				

				// find home location by getting the median of all the point that is of HOME cluster
				DescriptiveStatistics lat = new DescriptiveStatistics();
				DescriptiveStatistics lng = new DescriptiveStatistics();
				DescriptiveStatistics accuracy = new DescriptiveStatistics();
				for(ClusterableMobilityData d: cPoints){
					if(d.smoothedPlace == firstPlace && d.data.getLocation() != null){
						accuracy.addValue(d.data.getLocation().getAccuracy());
						lat.addValue(d.data.getLocation().getCoordinates().getLatitude());
						lng.addValue(d.data.getLocation().getCoordinates().getLongitude());
					}
				}
				Geo medianGeo = new Geo(lat.getPercentile(50), lng.getPercentile(50), true);
				GeoLocation homeLocation = new GeoLocation(cPoints.get(0).getTimestamp(), 
															medianGeo, 
															accuracy.getPercentile(50), 
															"MedianOfAllHomeGeoPoint");
				
				//	create data
				LeaveReturnHomeTimeData data = new LeaveReturnHomeTimeData(window, this)
												.setTimeLeaveHome(timeLeaveHome)
												.setHomeLocation(homeLocation)
												.setTimeReturnHome(timeReturnHome)
												.setScaledTimeAtHomeInSeconds(timeAtHome);
				// emit the record						
				this.createRecord()
						.setData(data)
						.setTimestamp(window.getFirstInstant())
						.emit();
			}


			
		}

		allPoints.clear();
	}
}
