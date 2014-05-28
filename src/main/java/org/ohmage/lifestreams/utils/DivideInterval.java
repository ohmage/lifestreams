package org.ohmage.lifestreams.utils;

import java.util.ArrayList;
import java.util.List;

import org.joda.time.DateTime;
import org.joda.time.Interval;


public class DivideInterval {
	static public List<Interval> byDay(Interval interval){
		List<Interval> segments = new ArrayList<Interval>();
		DateTime segmentStart = interval.getStart();
		DateTime segmentEnd = interval.getEnd();
		for (;segmentStart.isBefore(interval.getEnd());segmentStart = segmentEnd.plus(1)) {
			DateTime endOfTheDay = segmentStart.plusDays(1).withTimeAtStartOfDay().minusMillis(1);
			// make sure that the time span of a segment is not across the span of the current day 
			segmentEnd = interval.getEnd().isAfter(endOfTheDay) ? endOfTheDay : interval.getEnd();
			segments.add(new Interval(segmentStart, segmentEnd));
		}
		return segments;
	}
}
