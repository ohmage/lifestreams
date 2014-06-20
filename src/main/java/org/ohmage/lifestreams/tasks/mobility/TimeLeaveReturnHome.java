package org.ohmage.lifestreams.tasks.mobility;

import com.javadocmd.simplelatlng.LatLng;
import fr.dudie.nominatim.model.Address;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.Duration;
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

import java.util.LinkedList;

@Component
public class TimeLeaveReturnHome extends SimpleTimeWindowTask<MobilitySegment>{

	private LinkedList<PlaceSegment> data = new LinkedList<PlaceSegment>();
	public TimeLeaveReturnHome() {
		super(Days.ONE);
	}
	@Override
	public void executeDataPoint(StreamRecord<MobilitySegment> record, TimeWindow window) {
		if(record.getData().getSegmentType() == State.Place){
			data.add(record.getData().getPlaceSegment());
		}
	}
	@Override
	public void finishWindow(TimeWindow window) {
		if(data.isEmpty()){
			return;
		}

		PlaceSegment first = data.removeFirst();
		PlaceSegment last = data.size() > 0 ? data.removeLast() : first;
        long firstPlaceId = first.getAddress().getPlaceId();
        long lastPlaceId = last.getAddress().getPlaceId();

		// check the coverage
        long timeSpanInSecs = 0;
        for(PlaceSegment s: data)
            timeSpanInSecs += new Duration(s.getBegin(), s.getEnd()).getStandardSeconds();
        boolean enoughCoverage = first.getBegin().getHourOfDay() <= 11 && last.getEnd().getHourOfDay() >= 8;
        enoughCoverage &= timeSpanInSecs > 0.5 * Days.ONE.toStandardSeconds().getSeconds();
		// a home place must be the place which the user leave from and return to at the begin and the end of a day 
		boolean firstPlaceIdEqualToLast = firstPlaceId == lastPlaceId;

		if(enoughCoverage && firstPlaceIdEqualToLast){
            long homePlaceId = firstPlaceId;
            // extract home location
            Address homeAddr = first.getAddress();
            LatLng homeCor = new LatLng(homeAddr.getLatitude(), homeAddr.getLongitude());
            GeoLocation homeLocation = new GeoLocation(first.getBegin(), homeCor, -1, "PlaceDetection");
            // get the last segment before leaving home
            while(!data.isEmpty() && homePlaceId == data.getFirst().getAddress().getPlaceId()){
                first = data.removeFirst();
            }
            // get to the first segment after returning home
            while(!data.isEmpty() && homePlaceId == data.getLast().getAddress().getPlaceId()){
                last = data.removeLast();
            }
			long timeAtHome = 0L;
			getLogger().trace("{} Your home location: {}", first.getBegin().toLocalDate(), first.getAddress().getDisplayName());
            // compute time at home
            boolean everLeaveHome = !data.isEmpty();
			if(everLeaveHome){
                DateTime startOfDay = window.getTimeWindowBeginTime();
                DateTime endOfDay = window.getTimeWindowEndTime();
				timeAtHome += new Interval(startOfDay, first.getEnd()).toDurationMillis();
				timeAtHome += new Interval(last.getBegin(), endOfDay).toDurationMillis();
                // check if the user ever went back home before he return home at night
				for(PlaceSegment seg: data){
					if(seg.getAddress().getPlaceId() == homePlaceId){
						timeAtHome += new Interval(seg.getBegin(), seg.getEnd()).toDurationMillis();
					}
				}
			}else{
                // compute time at home
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
						  .setTimestamp(last.getEnd())
						  .emit();
			 
		}
		// clean up
		data.clear();
		this.checkpoint(window.getTimeWindowEndTime());
		
 	}

}
