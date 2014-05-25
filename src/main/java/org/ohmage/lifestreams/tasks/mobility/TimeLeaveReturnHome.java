package org.ohmage.lifestreams.tasks.mobility;

import java.util.LinkedList;
import java.util.List;

import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.Interval;
import org.ohmage.lifestreams.models.GeoLocation;
import org.ohmage.lifestreams.models.StreamRecord;
import org.ohmage.lifestreams.models.data.LeaveReturnHomeTimeData;
import org.ohmage.lifestreams.models.data.MobilitySegment;
import org.ohmage.lifestreams.models.data.MobilitySegment.PlaceSegment;
import org.ohmage.lifestreams.models.data.MobilitySegment.State;
import org.ohmage.lifestreams.tasks.SimpleTimeWindowTask;
import org.ohmage.lifestreams.tasks.TimeWindow;
import org.springframework.stereotype.Component;

@Component
public class TimeLeaveReturnHome extends SimpleTimeWindowTask<MobilitySegment>{


	LinkedList<PlaceSegment> data = new LinkedList<PlaceSegment>();
	GeoLocation homeLocation;
	Long totalTimeSpanInSec = 0L;
	public TimeLeaveReturnHome() {

		super(Days.ONE);
	}
	@Override
	public void executeDataPoint(StreamRecord<MobilitySegment> record, TimeWindow window) {
		totalTimeSpanInSec += record.getData().getTimeSpanInSecs(); 
		if(record.getData().getSegmentType() == State.Place){
			data.add(record.getData().getPlaceSegment());
			if(homeLocation == null){
				homeLocation = record.getLocation();
			}
		}
	}
	@Override
	public void finishWindow(TimeWindow window) {
		if(data.isEmpty()){
			return;
		}
		PlaceSegment first = data.removeFirst();
		PlaceSegment last = data.size() > 0 ? data.removeLast() : first;
		// check the coverage
		boolean enoughCoverage = first.getBegin().getHourOfDay() <= 11 && last.getEnd().getHourOfDay() >= 8;
		// a home place must be the place which the user leave from and return to at the begin and the end of a day 
		boolean firstPlaceIdEqualToLast = first.getAddress().getPlaceId() == last.getAddress().getPlaceId();
		boolean everLeaveHome = last != first;
		if(enoughCoverage && firstPlaceIdEqualToLast){
			long timeAtHome = 0L;
			getLogger().trace("{} Your home location: {}", first.getBegin().toLocalDate(), first.getAddress().getDisplayName());
			DateTime startOfDay = first.getBegin().withTimeAtStartOfDay();
			DateTime endOfDay = first.getBegin().plusDays(1).withTimeAtStartOfDay().minus(1);
			if(everLeaveHome){
				timeAtHome += new Interval(startOfDay, first.getEnd()).toDurationMillis();
				timeAtHome += new Interval(last.getBegin(), endOfDay).toDurationMillis();
				
				long homePlaceId = first.getAddress().getPlaceId();
				for(PlaceSegment seg: data){
					if(seg.getAddress().getPlaceId() == homePlaceId){
						timeAtHome += new Interval(seg.getBegin(), seg.getEnd()).toDurationMillis();
					}
				}
			}else{
				timeAtHome = Days.ONE.toStandardDuration().getMillis();
			}
			DateTime timeLeaveHome = everLeaveHome ? first.getEnd(): null;
			DateTime timeReturnHome = everLeaveHome ? last.getBegin(): null;
			LeaveReturnHomeTimeData data = new LeaveReturnHomeTimeData(window, this)
													.setScaledTimeAtHomeInSeconds((int) (timeAtHome/1000))
													.setTimeLeaveHome(timeLeaveHome)
													.setTimeReturnHome(timeReturnHome)
													.setHomeLocation(homeLocation);
			createRecord().setData(data)
						  .setLocation(homeLocation)
						  .setTimestamp(last.getEnd())
						  .emit();
			
		}
		// clean up
		homeLocation = null;
		data.clear();
		this.checkpoint(window.getTimeWindowEndTime());
		
 	}

}
