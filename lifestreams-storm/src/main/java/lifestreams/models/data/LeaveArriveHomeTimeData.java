package lifestreams.models.data;

import lifestreams.bolts.IGenerator;
import lifestreams.bolts.TimeWindow;
import lifestreams.models.GeoLocation;

import org.joda.time.DateTime;

public class LeaveArriveHomeTimeData extends LifestreamsData {
	public LeaveArriveHomeTimeData(TimeWindow window, IGenerator generator) {
		super(window, generator);
	}
	public int getTimeAtHomeInSeconds() {
		return timeAtHomeInSeconds;
	}
	public LeaveArriveHomeTimeData setTimeAtHomeInSeconds(int timeAtHomeInSeconds) {
		this.timeAtHomeInSeconds = timeAtHomeInSeconds;
		return this;
	}
	public DateTime getTimeLeaveHome() {
		return timeLeaveHome;
	}
	public LeaveArriveHomeTimeData setTimeLeaveHome(DateTime timeLeaveHome) {
		this.timeLeaveHome = timeLeaveHome;
		return this;
	}
	public DateTime getTimeArriveHome() {
		return timeArriveHome;
	}
	public LeaveArriveHomeTimeData setTimeArriveHome(DateTime timeArriveHome) {
		this.timeArriveHome = timeArriveHome;
		return this;
	}
	public GeoLocation getHomeLocation() {
		return homeLocation;
	}
	public LeaveArriveHomeTimeData setHomeLocation(GeoLocation homeLocation) {
		this.homeLocation = homeLocation;
		return this;
	}
	int timeAtHomeInSeconds;
	DateTime timeLeaveHome;
	DateTime timeArriveHome;
	GeoLocation homeLocation;
}
