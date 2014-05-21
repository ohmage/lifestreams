package org.ohmage.lifestreams.models.data;

import org.joda.time.DateTime;
import org.ohmage.lifestreams.bolts.IGenerator;
import org.ohmage.lifestreams.models.GeoLocation;
import org.ohmage.lifestreams.tasks.TimeWindow;

public class LeaveReturnHomeTimeData extends TimeWindowData {
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
