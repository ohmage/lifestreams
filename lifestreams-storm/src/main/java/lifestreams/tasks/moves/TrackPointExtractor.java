package lifestreams.tasks.moves;

import lifestreams.bolts.TimeWindow;
import lifestreams.models.GeoLocation;
import lifestreams.models.StreamRecord;
import lifestreams.models.data.LifestreamsData;
import lifestreams.tasks.SimpleTask;
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
public class TrackPointExtractor extends SimpleTask<MovesSegment> {
	class DummyMovesData extends LifestreamsData {
		public DummyMovesData(TimeWindow window, SimpleTask task) {
			super(window, task);
		}

	}

	private void emitTrackPoint(TrackPoint point, MovesSegment segment) {
		Geo coordinates = new Geo(point.getLat(), point.getLon(), true);
		GeoLocation location = new GeoLocation(point.getTimestamp(),
				coordinates, 0, "Moves");
		StreamRecord<DummyMovesData> record = new StreamRecord<DummyMovesData>(
				getUser(), point.getTimestamp(), location, new DummyMovesData(
						null, this));
		this.emit(record);
	}

	@Override
	public void executeDataPoint(StreamRecord<MovesSegment> dp,
			TimeWindow window) {
		
		if (dp.d().getActivities() != null) {
			for (MovesActivity activity : dp.d().getActivities()) {
				if (activity != null && activity.getTrackPoints() != null) {
					for (TrackPoint point : activity.getTrackPoints()) {
						if (point.getTimestamp() == null)
							point.setTimestamp(activity.getEndTime());
						emitTrackPoint(point, dp.d());
					}
				}
			}
		}
		if (dp.d().getPlace() != null
				&& dp.d().getPlace().getLocation() != null) {
			dp.d().getPlace().getLocation().setTimestamp(dp.d().getEndTime());
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
