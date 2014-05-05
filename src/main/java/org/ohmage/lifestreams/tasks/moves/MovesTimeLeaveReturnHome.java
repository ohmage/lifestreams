package org.ohmage.lifestreams.tasks.moves;

import java.util.ArrayList;

import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.Interval;
import org.ohmage.lifestreams.models.GeoLocation;
import org.ohmage.lifestreams.models.StreamRecord;
import org.ohmage.lifestreams.models.data.LeaveReturnHomeTimeData;
import org.ohmage.lifestreams.tasks.SimpleTimeWindowTask;
import org.ohmage.lifestreams.tasks.TimeWindow;
import org.springframework.stereotype.Component;

import co.nutrino.api.moves.impl.dto.activity.TrackPoint;
import co.nutrino.api.moves.impl.dto.storyline.MovesPlace;
import co.nutrino.api.moves.impl.dto.storyline.MovesPlaceTypeEnum;
import co.nutrino.api.moves.impl.dto.storyline.MovesSegment;

import com.bbn.openmap.geo.Geo;

@Component
public class MovesTimeLeaveReturnHome extends SimpleTimeWindowTask<MovesSegment> {


	ArrayList<MovesSegment> segments = new ArrayList<MovesSegment>();
	static final float minimunCoverageRate = (float)0.5;
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
			int timeAtHomeInSeconds = 0;
			DateTime timeLeaveHome = null;
			DateTime timeReturnHome = null;
			GeoLocation homeLocation =null;
			
			MovesPlace lastPlace = null;
			DateTime lastPlaceEndTime = null;
			// check if Moves recognize home location, and track the first/last place Id
			long homePlaceId = -1;
			long firstPlaceId = -1;
			long lastPlaceId = -1;
			for(MovesSegment segment: segments){
				if(segment.getPlace() != null){
					 if(segment.getPlace().getType().equals(MovesPlaceTypeEnum.Home)){
						 homePlaceId = segment.getPlace().getId();
					 }
					 if(firstPlaceId == -1){
						 firstPlaceId = segment.getPlace().getId();
					 }
					 lastPlaceId = segment.getPlace().getId();
				}
			}
			if(homePlaceId == -1){
				// if Moves does not recognize home location, check if first place == last place
				if(firstPlaceId == lastPlaceId){
					// if so, set that place as home place
					homePlaceId = firstPlaceId;
					for(MovesSegment segment: segments){
						if(segment.getPlace() != null && segment.getPlace().getId() == homePlaceId ){
							segment.getPlace().setType(MovesPlaceTypeEnum.Home);
						}
					}
				}
			}
			for(MovesSegment segment: segments){
				if(segment.getPlace() != null){
					// if it is a place segment
					MovesPlace curPlace = segment.getPlace();
					if(timeLeaveHome == null && lastPlace != null 
							&& curPlace.getType() != lastPlace.getType() && lastPlace.getType().equals(MovesPlaceTypeEnum.Home)){
						// set the time leave home as the end time of the first home segment whose next segment is not at home
						timeLeaveHome = lastPlaceEndTime;
					}
					if(segment.getPlace().getType().equals(MovesPlaceTypeEnum.Home)){
						// if it is a Place segment and it is at Home
						timeAtHomeInSeconds += new Interval(segment.getStartTime(), segment.getEndTime()).toDurationMillis() / 1000;
						if(lastPlace != null && curPlace.getType() != lastPlace.getType() && curPlace.getType().equals(MovesPlaceTypeEnum.Home)){
							// set the time return home as the start time of the last home segment whose previous segment is not at home
							timeReturnHome = segment.getStartTime();
						}
						
						// if homeLocation is not set
						if(homeLocation == null && segment.getPlace().getLocation() != null){
							// get the location of the home
							TrackPoint geoPoint = segment.getPlace().getLocation();
							Geo coordinates = new Geo(geoPoint.getLat(), geoPoint.getLon(), true);
							homeLocation = new GeoLocation(segment.getStartTime(),
									coordinates, -1, "Moves");
						}
						
					}
					lastPlace = segment.getPlace();
					lastPlaceEndTime = segment.getEndTime();
				}
			}
			if(timeAtHomeInSeconds > 0 && timeReturnHome != null && timeLeaveHome != null){
				LeaveReturnHomeTimeData data = new LeaveReturnHomeTimeData(window, this)
							.setHomeLocation(homeLocation)
							.setTimeReturnHome(timeReturnHome)
							.setTimeLeaveHome(timeLeaveHome)
							.setScaledTimeAtHomeInSeconds((int)(timeAtHomeInSeconds / coverageRate));
				this.createRecord()
						.setData(data)
						.setTimestamp(window.getFirstInstant()).emit();;
			}
		}
		segments.clear();
	}


}
