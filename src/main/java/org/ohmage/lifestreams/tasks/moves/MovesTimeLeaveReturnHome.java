package org.ohmage.lifestreams.tasks.moves;

import co.nutrino.api.moves.impl.dto.storyline.MovesPlace;
import co.nutrino.api.moves.impl.dto.storyline.MovesPlaceTypeEnum;
import co.nutrino.api.moves.impl.dto.storyline.MovesSegment;
import com.javadocmd.simplelatlng.LatLng;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.ohmage.lifestreams.models.GeoLocation;
import org.ohmage.lifestreams.models.StreamRecord;
import org.ohmage.lifestreams.models.data.LeaveReturnHomeTimeData;
import org.ohmage.lifestreams.tasks.SimpleTimeWindowTask;
import org.ohmage.lifestreams.tasks.TimeWindow;
import org.springframework.stereotype.Component;

import java.util.LinkedList;

@Component
public class MovesTimeLeaveReturnHome extends SimpleTimeWindowTask<MovesSegment> {


	private LinkedList<MovesSegment> segments = new LinkedList<MovesSegment>();
	private static final float minimunCoverageRate = (float)0.5;
	@Override
	public void executeDataPoint(StreamRecord<MovesSegment> record,	TimeWindow window) {
		segments.add(record.d());
	}

	@Override
	public void finishWindow(TimeWindow window) {
		// first check if we have > 50% coverage
		long coverageInMilliSeconds = 0;
		for(MovesSegment segment: segments){
			coverageInMilliSeconds += new Interval(segment.getStartTime(), segment.getEndTime()).toDurationMillis();
		}
		float coverageRate = ((float)coverageInMilliSeconds) / (window.getTimeWindowSizeInSecond() * 1000);
		if(coverageRate > minimunCoverageRate){
			// check if Moves recognize home location, and track the first/last place Id
			long homePlaceId = -1;
            LinkedList<MovesSegment> placeSegments = new LinkedList<MovesSegment>();
            for(MovesSegment segment: segments){
                if(segment.getPlace() != null) {
                    placeSegments.add(segment);
                }
            }
            // use the home place id recognized by Moves
			for(MovesSegment segment: placeSegments){
                 if(segment.getPlace().getType().equals(MovesPlaceTypeEnum.Home)){
                     homePlaceId = segment.getPlace().getId();
                 }
			}
            // if no home place is recognized, use firstPlace=lastPlace=homePlace assumption
			if(homePlaceId == -1){
				// if Moves does not recognize home location, check if first place == last place
                long firstPlaceId = placeSegments.getFirst().getPlace().getId();
                long lastPlaceId = placeSegments.getLast().getPlace().getId();
				if(firstPlaceId == lastPlaceId){
					// if so, set that place as home place
					homePlaceId = firstPlaceId;
				}
			}
            if(homePlaceId != -1) {
                GeoLocation homeLocation = null;
                for(MovesSegment segment: placeSegments) {
                    if (segment.getPlace().getId() == homePlaceId) {
                        segment.getPlace().setType(MovesPlaceTypeEnum.Home);
                        if(homeLocation == null) {
                            double lat = segment.getPlace().getLocation().getLat();
                            double lng = segment.getPlace().getLocation().getLon();
                            homeLocation = new GeoLocation(segment.getStartTime(), new LatLng(lat, lng), -1, "Moves");
                        }
                    }
                }
                DateTime timeLeaveHome = null,  timeReturnHome = null;
                boolean everLeaveHome = false;
                while(!segments.isEmpty()){
                    MovesSegment segment = segments.getFirst();
                    MovesPlace curPlace = segment.getPlace();
                    if (curPlace != null &&  curPlace.getType().equals(MovesPlaceTypeEnum.Home)) {
                        // set the time leave home as the end time of
                        // the first home segment whose next segment is not at home
                        timeLeaveHome = segment.getEndTime();
                        segments.removeFirst();
                    } else {
                        everLeaveHome = true;
                        break;
                    }

                }
                while(!segments.isEmpty()){
                    MovesSegment segment = segments.getLast();
                    MovesPlace curPlace = segment.getPlace();
                    if (curPlace != null &&
                            curPlace.getType().equals(MovesPlaceTypeEnum.Home)) {
                        // set the time return home as the start time of the first home segment whose
                        // every following segment is at home and the previous segment is not at home
                        timeReturnHome = segment.getStartTime();
                        segments.removeLast();
                    } else {
                        everLeaveHome = true;
                        break;
                    }

                }
                int timeAtHomeInSecs = 0;
                if(everLeaveHome && timeLeaveHome != null && timeReturnHome != null) {
                    timeAtHomeInSecs += new Duration(window.getTimeWindowBeginTime(), timeLeaveHome).getStandardSeconds();
                    timeAtHomeInSecs += new Duration(timeReturnHome, window.getTimeWindowEndTime()).getStandardSeconds();
                    for(MovesSegment segment: segments) {
                        MovesPlace curPlace = segment.getPlace();
                        if (curPlace != null && curPlace.getType().equals(MovesPlaceTypeEnum.Home)) {
                            timeAtHomeInSecs += new Duration(segment.getStartTime(),
                                    segment.getEndTime()).getStandardSeconds();
                        }
                    }
                }else {
                    timeLeaveHome = timeReturnHome = null;
                    timeAtHomeInSecs = Days.ONE.toStandardSeconds().getSeconds();
                }

                LeaveReturnHomeTimeData data = new LeaveReturnHomeTimeData(window, this)
                        .setHomeLocation(homeLocation)
                        .setTimeReturnHome(timeReturnHome)
                        .setTimeLeaveHome(timeLeaveHome)
                        .setScaledTimeAtHomeInSeconds(timeAtHomeInSecs);
                getLogger().trace("{} {} {}", data.getTimeLeaveHome(), data.getTimeReturnHome(),
                        timeAtHomeInSecs / 3600.0);

                this.createRecord()
                        .setData(data)
                        .setTimestamp(window.getFirstInstant()).emit();
            }
		}

        segments.clear();
		checkpoint(window.getTimeWindowEndTime());
	}


}
