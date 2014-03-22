package lifestreams.models.data;

import java.util.ArrayList;
import java.util.List;

import lifestreams.bolts.IGenerator;
import lifestreams.bolts.TimeWindow;

public class ActivitySummaryData extends LifestreamsData {

	public double getTotalTime() {
		return totalTime;
	}

	public ActivitySummaryData setTotalTimeInSeconds(double totalTime) {
		this.totalTime = totalTime;
		return this;
	}

	public double getTotalActiveTimeInSeconds() {
		return totalActiveTime;
	}

	public ActivitySummaryData setTotalActiveTimeInSeconds(double totalActiveTime) {
		this.totalActiveTime = totalActiveTime;
		return this;
	}

	public double getTotalSedentaryTimeInSeconds() {
		return totalSedentaryTime;
	}

	public ActivitySummaryData setTotalSedentaryTimeInSeconds(double totalSedentaryTime) {
		this.totalSedentaryTime = totalSedentaryTime;
		return this;
	}

	public double getTotalTransportationTimeInSeconds() {
		return totalTransportationTime;
	}

	public ActivitySummaryData setTotalTransportationTimeInSeconds(
			double totalTransportationTime) {
		this.totalTransportationTime = totalTransportationTime;
		return this;
	}

	public List<ActivityEpisode> getActivityEpisodes() {
		return activityEpisodes;
	}

	public ActivitySummaryData setActivityEpisodes(
			List<ActivityEpisode> activeInstances) {
		this.activityEpisodes = activeInstances;
		return this;
	}

	double totalTime;
	double totalActiveTime;
	double totalSedentaryTime;
	double totalTransportationTime;

	List<ActivityEpisode> activityEpisodes = new ArrayList<ActivityEpisode>();

	public ActivitySummaryData(TimeWindow window, IGenerator generator) {
		super(window, generator);
	}

}
