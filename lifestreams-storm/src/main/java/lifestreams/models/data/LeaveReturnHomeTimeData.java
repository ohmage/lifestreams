package lifestreams.models.data;

import lifestreams.bolts.IGenerator;
import lifestreams.bolts.TimeWindow;
import lifestreams.models.GeoLocation;

import org.joda.time.DateTime;

public class LeaveReturnHomeTimeData extends LifestreamsData {
	public LeaveReturnHomeTimeData(TimeWindow window, IGenerator generator) {
		super(window, generator);
	}
	public int getScaledTimeAtHomeInSeconds() {
		return timeAtHomeInSeconds;
	}
	public LeaveReturnHomeTimeData setScaledTimeAtHomeInSeconds(int timeAtHomeInSeconds) {
		this.timeAtHomeInSeconds = timeAtHomeInSeconds;
		return this;
	}
	public DateTime getTimeLeaveHome() {
		return timeLeaveHome;
	}
	public LeaveReturnHomeTimeData setTimeLeaveHome(DateTime timeLeaveHome) {
		this.timeLeaveHome = timeLeaveHome;
		return this;
	}
	public DateTime getTimeReturnHome() {
		return timeReturneHome;
	}
	public LeaveReturnHomeTimeData setTimeReturnHome(DateTime timeReturnHome) {
		this.timeReturneHome = timeReturnHome;
		return this;
	}
	public GeoLocation getHomeLocation() {
		return homeLocation;
	}
	public LeaveReturnHomeTimeData setHomeLocation(GeoLocation homeLocation) {
		this.homeLocation = homeLocation;
		return this;
	}
	int timeAtHomeInSeconds;
	DateTime timeLeaveHome;
	DateTime timeReturneHome;
	GeoLocation homeLocation;
}
