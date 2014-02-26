package lifestreams.model;

import java.util.ArrayList;
import java.util.List;

import lifestreams.bolt.BaseLifestreamsBolt;
import lifestreams.utils.TimeWindow;

import org.joda.time.DateTime;
import org.ohmage.models.OhmageUser;

public class ActivitySummaryDataPoint extends LifestreamsDataPoint{
	
	public double getTotalTime() {
		return totalTime;
	}


	public ActivitySummaryDataPoint setTotalTime(double totalTime) {
		this.totalTime = totalTime;
		return this;
	}


	public double getTotalActiveTime() {
		return totalActiveTime;
	}


	public ActivitySummaryDataPoint setTotalActiveTime(double totalActiveTime) {
		this.totalActiveTime = totalActiveTime;
		return this;
	}


	public double getTotalSedentaryTime() {
		return totalSedentaryTime;
	}


	public ActivitySummaryDataPoint setTotalSedentaryTime(double totalSedentaryTime) {
		this.totalSedentaryTime = totalSedentaryTime;
		return this;
	}


	public double getTotalTransportationTime() {
		return totalTransportationTime;
	}


	public ActivitySummaryDataPoint setTotalTransportationTime(double totalTransportationTime) {
		this.totalTransportationTime = totalTransportationTime;
		return this;
	}


	public List<ActivityInstance> getActivityInstances() {
		return activityInstances;
	}

	public ActivitySummaryDataPoint setActivityInstances(List<ActivityInstance> activeInstances) {
		this.activityInstances = activeInstances;
		return this;
	}
	
	double totalTime;
	double totalActiveTime;
	double totalSedentaryTime;
	double totalTransportationTime;
	
	List<ActivityInstance> activityInstances = new ArrayList<ActivityInstance> ();
	
	
	public ActivitySummaryDataPoint(OhmageUser user, TimeWindow window,
			BaseLifestreamsBolt generator) {
		super(user, window.getLastInstant(), window, generator);
	}

}
