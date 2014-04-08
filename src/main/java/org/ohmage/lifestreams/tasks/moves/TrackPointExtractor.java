package org.ohmage.lifestreams.tasks.moves;

import org.ohmage.lifestreams.bolts.TimeWindow;
import org.ohmage.lifestreams.models.GeoLocation;
import org.ohmage.lifestreams.models.StreamRecord;
import org.ohmage.lifestreams.models.data.LifestreamsData;
import org.ohmage.lifestreams.tasks.SimpleTask;
import org.springframework.stereotype.Component;

import co.nutrino.api.moves.impl.dto.activity.MovesActivity;
import co.nutrino.api.moves.impl.dto.activity.TrackPoint;
import co.nutrino.api.moves.impl.dto.storyline.MovesSegment;

import com.bbn.openmap.geo.Geo;

/**
 * @author changun This task extracts tracking points from MovesSegements and
 *         outputs dummy stream records with corresponding timestamp and geo
 *         location information.
 * 
 */
@Component
public class TrackPointExtractor extends SimpleTask<MovesSegment> {
	class DummyMovesTrackPointData extends LifestreamsData {
		public DummyMovesTrackPointData(TrackPointExtractor generator) {
			super(null, generator);
		}

	}

	private void emitTrackPoint(TrackPoint point, MovesSegment segment) {
		Geo coordinates = new Geo(point.getLat(), point.getLon(), true);
		GeoLocation location = new GeoLocation(point.getTime(),
				coordinates, -1, "Moves");
		this.createRecord()
				.setData(new DummyMovesTrackPointData(this))
				.setLocation(location)
				.setTimestamp(point.getTime()).emit();
	}

	@Override
	public void executeDataPoint(StreamRecord<MovesSegment> dp,
			TimeWindow window) {
		
		if (dp.d().getActivities() != null) {
			// if this segment contains activities
			for (MovesActivity activity : dp.d().getActivities()) {
				// if this activity contains tracking points
				if (activity != null && activity.getTrackPoints() != null) {
					for (TrackPoint point : activity.getTrackPoints()) {
						// pull out these track points
						if (point.getTime() == null){
							// set the timestamp as the timestamp of the activity
							point.setTime(activity.getEndTime());
						}
						// emit this track point
						emitTrackPoint(point, dp.d());
					}
				}
			}
		}
		if (dp.d().getPlace() != null && dp.d().getPlace().getLocation() != null) {
			// if this segment contains a place
			dp.d().getPlace().getLocation().setTime(dp.d().getEndTime());
			// emit the location of this place as a tracking point
			emitTrackPoint(dp.d().getPlace().getLocation(), dp.d());
		}
	}

	@Override
	public void finishWindow(TimeWindow window) {
		// TODO Auto-generated method stub

	}

	@Override
	public void snapshotWindow(TimeWindow window) {
		// TODO Auto-generated method stub

	}

}
