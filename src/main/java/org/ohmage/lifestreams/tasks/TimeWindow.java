package org.ohmage.lifestreams.tasks;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;

import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.Duration;
import org.joda.time.Hours;
import org.joda.time.Interval;
import org.joda.time.Minutes;
import org.joda.time.Months;
import org.joda.time.MutableDateTime;
import org.joda.time.Seconds;
import org.joda.time.Weeks;
import org.joda.time.Years;
import org.joda.time.base.BaseSingleFieldPeriod;

public class TimeWindow {
	static final int minimunSamplingIntervalInSec = 30;
	BaseSingleFieldPeriod windowDuration;
	DateTime firstInstant;
	DateTime lastInstant;
	private DateTime epoch;
	// the instants at which we have data point
	// it is used to compute the median sampling period and missing data rate
	transient BitSet instantSet; 
	public TimeWindow(BaseSingleFieldPeriod duration, DateTime time) {
		this.windowDuration = duration;
		this.firstInstant = time;
		this.lastInstant = time;
		// get the epoch (of the first instant's time zone)
		MutableDateTime start = new MutableDateTime();
		start.setZone(time.getZone());
		start.setDate(0);
		start.setTime(0);
		this.epoch = new DateTime(start);
		this.instantSet = new BitSet(windowDuration.toPeriod().toStandardSeconds().getSeconds() / minimunSamplingIntervalInSec);
		
		instantSet.set((int) (new Duration(this.getTimeWindowBeginTime(), time).getStandardSeconds() / minimunSamplingIntervalInSec));
	}

	public void update(DateTime newTime) {
		if (newTime.isBefore(firstInstant)) {
			this.firstInstant = newTime;
		} else if (newTime.isAfter(lastInstant)) {
			this.lastInstant = newTime;
		}
		instantSet.set((int) (new Duration(this.getTimeWindowBeginTime(), newTime).getStandardSeconds() / minimunSamplingIntervalInSec));
	}
	private DateTime timeWindowBeginTime;
	public DateTime getTimeWindowBeginTime(){
		if(timeWindowBeginTime == null){
			MutableDateTime start = new MutableDateTime(this.firstInstant);
			while(this.withinWindow(start.toDateTime())){
				start.addMinutes(-1);
			}
			start.addMinutes(1);
			
			while(this.withinWindow(start.toDateTime())){
				start.addSeconds(-1);
			}
			start.addSeconds(1);
			
			start.setMillisOfSecond(0);
			timeWindowBeginTime = start.toDateTime();
		}
		return timeWindowBeginTime;
	}
	public DateTime getTimeWindowEndTime(){
		return this.getTimeWindowBeginTime().plus(this.windowDuration).minus(1);
	}
	public int getTimeWindowSizeInSecond(){
		return this.windowDuration.toPeriod().toStandardSeconds().getSeconds();
	}
	public DateTime getFirstInstant() {
		return firstInstant;
	}

	public DateTime getLastInstant() {
		return lastInstant;
	}
	public long getMedianSamplingIntervalInSecond() {
		if(instantSet.cardinality() < 2)
			return windowDuration.toPeriod().toStandardSeconds().getSeconds();
		List<Long> intervals = new ArrayList<Long>();
		int prevSetBit = this.instantSet.nextSetBit(0);
		 for (int i = this.instantSet.nextSetBit(prevSetBit); i >= 0; i = this.instantSet.nextSetBit(i+1)) {
		     // operate on index i here
			 intervals.add((long) ((i-prevSetBit) * minimunSamplingIntervalInSec));
		 }

		Collections.sort(intervals);
		return intervals.get(intervals.size()/2);
	}
	public double getHeuristicMissingDataRate() {
		if(instantSet == null){
			return -1;
		}
		if(instantSet.size() < 2)
			return 1;
		Long secondsInTheTimeWindow = new Interval(this.getTimeWindowBeginTime(), this.getTimeWindowEndTime()).toDurationMillis() / 1000;
		double numOfSamplesIfFullCoverage = (double)secondsInTheTimeWindow / (double)getMedianSamplingIntervalInSecond();
		return 1 - (instantSet.size() / numOfSamplesIfFullCoverage);
	}
	
	public long numOfTimeWindowSinceEpoch(DateTime dt){
		// TODO: resolve timezone problem. 
		// right now we use the timezone of the first received data point in the time window 
		if (windowDuration.getClass() == Years.class)
			return Years.yearsBetween(epoch, dt).getYears();
		else if (windowDuration.getClass() == Months.class)
			return Months.monthsBetween(epoch, dt).getMonths();
		else if (windowDuration.getClass() == Weeks.class)
			return Weeks.weeksBetween(epoch, dt).getWeeks();
		else if (windowDuration.getClass() == Days.class) 
			return Days.daysBetween(epoch, dt).getDays();			
		else if (windowDuration.getClass() == Hours.class) 
			return Hours.hoursBetween(epoch, dt).getHours();
		else if (windowDuration.getClass() == Minutes.class) 
			return Minutes.minutesBetween(epoch, dt).getMinutes();
		else if (windowDuration.getClass() == Seconds.class)
			return Seconds.secondsBetween(epoch, dt).getSeconds();
		throw new RuntimeException("type of time window is not supported");
	}
	public boolean withinWindow(DateTime dt) {
		return (numOfTimeWindowSinceEpoch(firstInstant) == numOfTimeWindowSinceEpoch(dt));
	}
}
