package lifestreams.models.data;

import java.util.ArrayList;
import java.util.List;

import lifestreams.bolts.IGenerator;
import lifestreams.bolts.TimeWindow;

public class ActivitySummaryData extends LifestreamsData {

	public double getTotalTime() {
		return totalTime;
	}

	public ActivitySummaryData setTotalTime(double totalTime) {
		this.totalTime = totalTime;
		return this;
	}

	public double getTotalActiveTime() {
		return totalActiveTime;
	}

	public ActivitySummaryData setTotalActiveTime(double totalActiveTime) {
		this.totalActiveTime = totalActiveTime;
		return this;
	}

	public double getTotalSedentaryTime() {
		return totalSedentaryTime;
	}

	public ActivitySummaryData setTotalSedentaryTime(double totalSedentaryTime) {
		this.totalSedentaryTime = totalSedentaryTime;
		return this;
	}

	public double getTotalTransportationTime() {
		return totalTransportationTime;
	}

	public ActivitySummaryData setTotalTransportationTime(
			double totalTransportationTime) {
		this.totalTransportationTime = totalTransportationTime;
		return this;
	}

	public List<ActivityInstance> getActivityInstances() {
		return activityInstances;
	}

	public ActivitySummaryData setActivityInstances(
			List<ActivityInstance> activeInstances) {
		this.activityInstances = activeInstances;
		return this;
	}

	double totalTime;
	double totalActiveTime;
	double totalSedentaryTime;
	double totalTransportationTime;

	List<ActivityInstance> activityInstances = new ArrayList<ActivityInstance>();

	public ActivitySummaryData(TimeWindow window, IGenerator generator) {
		super(window, generator);
	}

}
