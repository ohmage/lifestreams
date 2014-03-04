package lifestreams.utils;

import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.Hours;
import org.joda.time.Minutes;
import org.joda.time.Months;
import org.joda.time.MutableDateTime;
import org.joda.time.Seconds;
import org.joda.time.Weeks;
import org.joda.time.Years;
import org.joda.time.base.BaseSingleFieldPeriod;

public class TimeWindow {
	BaseSingleFieldPeriod windowDuration;
	DateTime firstInstant;
	DateTime lastInstant;
	private DateTime start;

	public TimeWindow(BaseSingleFieldPeriod duration, DateTime time) {
		this.windowDuration = duration;
		this.firstInstant = time;
		this.lastInstant = time;
		// get epochstart
		MutableDateTime start = new MutableDateTime();
		start.setZone(time.getZone());
		start.setDate(0);
		start.setTime(0);
		this.start = new DateTime(start);
	}

	public void update(DateTime newTime) {
		if (newTime.isBefore(firstInstant)) {
			this.firstInstant = newTime;
		} else if (newTime.isAfter(lastInstant)) {
			this.lastInstant = newTime;
		}
	}

	public DateTime getFirstInstant() {
		return firstInstant;
	}

	public DateTime getLastInstant() {
		return lastInstant;
	}

	public boolean withinWindow(DateTime dt2) {
		// TODO: resolve timezone problem. Different datapoint might have
		// different timezones
		// right now we use the timezone of the first data point in the buffer

		// then check, with epoch as the beginning, if the dt1 and dt2 are of
		// the same period
		int periodValue = windowDuration.getValue(0);
		if (windowDuration.getClass() == Years.class
				&& Math.abs(Years.yearsBetween(start, firstInstant).getYears()
						- Years.yearsBetween(start, dt2).getYears()) < periodValue)
			return true;
		else if (windowDuration.getClass() == Months.class
				&& Math.abs(Months.monthsBetween(start, firstInstant)
						.getMonths()
						- Months.monthsBetween(start, dt2).getMonths()) < periodValue)
			return true;
		else if (windowDuration.getClass() == Weeks.class
				&& Math.abs(Weeks.weeksBetween(start, firstInstant).getWeeks()
						- Weeks.weeksBetween(start, dt2).getWeeks()) < periodValue)
			return true;
		else if (windowDuration.getClass() == Days.class
				&& Math.abs(Days.daysBetween(start, firstInstant).getDays()
						- Days.daysBetween(start, dt2).getDays()) < periodValue)
			return true;
		else if (windowDuration.getClass() == Hours.class
				&& Math.abs(Hours.hoursBetween(start, firstInstant).getHours()
						- Hours.hoursBetween(start, dt2).getHours()) < periodValue)
			return true;
		else if (windowDuration.getClass() == Minutes.class
				&& Math.abs(Minutes.minutesBetween(start, firstInstant)
						.getMinutes()
						- Minutes.minutesBetween(start, dt2).getMinutes()) < periodValue)
			return true;
		else if (windowDuration.getClass() == Seconds.class
				&& Math.abs(Seconds.secondsBetween(start, firstInstant)
						.getSeconds()
						- Seconds.secondsBetween(start, dt2).getSeconds()) < periodValue)
			return true;
		else
			return false;
	}
}
