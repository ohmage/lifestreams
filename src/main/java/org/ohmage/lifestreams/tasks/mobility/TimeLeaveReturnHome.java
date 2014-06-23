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
import org.ohmage.lifestreams.models.data.MobilitySegment.Segment;
import org.ohmage.lifestreams.tasks.SimpleTimeWindowTask;
import org.ohmage.lifestreams.tasks.TimeWindow;
import org.springframework.stereotype.Component;

import java.util.LinkedList;

@Component
public class TimeLeaveReturnHome extends SimpleTimeWindowTask<MobilitySegment>{

	private LinkedList<Segment> segs = new LinkedList<Segment>();
	public TimeLeaveReturnHome() {
		super(Days.ONE);
	}
	@Override
	public void executeDataPoint(StreamRecord<MobilitySegment> record, TimeWindow window) {
        segs.add(record.getData().getSegment());
	}
	@Override
	public void finishWindow(TimeWindow window) {
        // remove head/tail segments if they are Moves
        while(!segs.isEmpty() && segs.getFirst().getState() == MobilitySegment.State.Moves){
            segs.removeFirst();
        }
        while(!segs.isEmpty() && segs.getLast().getState() == MobilitySegment.State.Moves){
            segs.removeLast();
        }
        // check if it empty after remove those segments
        if(segs.isEmpty()){
            return;
        }
        // check the coverage
        long timeSpanInSecs = 0;
        for(Segment s: segs) {
            timeSpanInSecs += s.getTimeSpanInSecs();
        }

        boolean enoughCoverage = segs.getFirst().getBegin().getHourOfDay() <= 11 &&
                                 segs.getLast().getEnd().getHourOfDay()   >= 20 &&
                                 timeSpanInSecs > 0.5 * Days.ONE.toStandardSeconds().getSeconds();

        /*** check if first place id == last place id ***/
        PlaceSegment first = (PlaceSegment)segs.removeFirst();
		PlaceSegment last = segs.size() > 0 ? (PlaceSegment)segs.removeLast() : first;
        long firstPlaceId = first.getAddress().getPlaceId();
        long lastPlaceId = last.getAddress().getPlaceId();

		// a home place must be the place which the user leave from and return to at the begin and the end of a day
		boolean firstPlaceIdEqualToLast = firstPlaceId == lastPlaceId;

		if(enoughCoverage && firstPlaceIdEqualToLast){
            long homePlaceId = firstPlaceId;
            // extract home geolocation
            Address homeAddr = first.getAddress();
            LatLng homeCor = new LatLng(homeAddr.getLatitude(), homeAddr.getLongitude());
            GeoLocation homeLocation = new GeoLocation(first.getBegin(), homeCor, -1, "PlaceDetection");
            PlaceSegment leave = first;
            PlaceSegment ret = last;
            // get the last segment before leaving home
            while(!segs.isEmpty() && segs.getFirst().getState() == MobilitySegment.State.Place){
                PlaceSegment placeSeg = ((PlaceSegment)segs.getFirst());
                 if(placeSeg.getAddress().getPlaceId() == homePlaceId) {
                     leave = (PlaceSegment)segs.removeFirst();
                 }else{
                     break;
                 }
            }
            // get the first segment after returning home
            while(!segs.isEmpty() && segs.getLast().getState() == MobilitySegment.State.Place){
                PlaceSegment placeSeg = ((PlaceSegment)segs.getLast());
                if(placeSeg.getAddress().getPlaceId() == homePlaceId) {
                    ret = (PlaceSegment)segs.removeLast();
                }else{
                    break;
                }
            }
            boolean everLeaveHome = !segs.isEmpty();
            DateTime timeLeaveHome = everLeaveHome ? leave.getEnd(): null;
            DateTime timeReturnHome = everLeaveHome ? ret.getBegin(): null;
			long timeAtHome = 0L;
			getLogger().trace("{} Your home location: {}", first.getBegin().toLocalDate(), first.getAddress().getDisplayName());
            // compute time at home
			if(everLeaveHome){
                DateTime startOfDay = window.getTimeWindowBeginTime();
                DateTime endOfDay = window.getTimeWindowEndTime();
				timeAtHome += new Interval(startOfDay, timeLeaveHome).toDurationMillis();
				timeAtHome += new Interval(timeReturnHome, endOfDay).toDurationMillis();
                // check if the user ever went back home before he return home at night
				for(Segment seg: segs){
                    if(seg.getState() == MobilitySegment.State.Place) {
                        PlaceSegment placeSeg = (PlaceSegment)seg;
                        // it is a Place segment and is at home
                        if (placeSeg.getAddress().getPlaceId() == homePlaceId) {
                            timeAtHome += new Interval(seg.getBegin(), seg.getEnd()).toDurationMillis();
                        }
                    }
				}
			}else{
                // compute time at home
				timeAtHome = Days.ONE.toStandardDuration().getMillis();
			}


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
		segs.clear();
		this.checkpoint(window.getTimeWindowEndTime());
		
 	}

}
