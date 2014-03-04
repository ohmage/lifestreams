package lifestreams.bolt.moves;

import com.bbn.openmap.geo.Geo;

import backtype.storm.topology.BasicOutputCollector;
import co.nutrino.api.moves.impl.dto.activity.MovesActivity;
import co.nutrino.api.moves.impl.dto.activity.TrackPoint;
import co.nutrino.api.moves.impl.dto.storyline.MovesSegment;
import lifestreams.bolt.SimpleTask;
import lifestreams.model.GeoLocation;
import lifestreams.model.StreamRecord;
import lifestreams.model.data.LifestreamsData;
import lifestreams.utils.TimeWindow;

public class TrackPointExtractor extends SimpleTask<MovesSegment> {
	class DummyMovesData extends LifestreamsData {
		public DummyMovesData(TimeWindow window, SimpleTask task) {
			super(window, task);
		}

	}

	private void emitTrackPoint(TrackPoint point, MovesSegment segment,
			BasicOutputCollector collector) {
		Geo coordinates = new Geo(point.getLat(), point.getLon(), true);
		GeoLocation location = new GeoLocation(point.getTimestamp(),
				coordinates, 0, "Moves");
		StreamRecord<DummyMovesData> record = new StreamRecord<DummyMovesData>(
				getUser(), point.getTimestamp(), location, new DummyMovesData(
						null, this));
		this.emit(record, collector);
	}

	@Override
	public void executeDataPoint(StreamRecord<MovesSegment> dp,
			TimeWindow window, BasicOutputCollector collector) {
		if (dp.d().getActivities() != null) {
			for (MovesActivity activity : dp.d().getActivities()) {
				if (activity != null && activity.getTrackPoints() != null) {
					for (TrackPoint point : activity.getTrackPoints()) {
						if (point.getTimestamp() == null)
							point.setTimestamp(activity.getEndTime());
						emitTrackPoint(point, dp.d(), collector);
					}
				}
			}
		}
		if (dp.d().getPlace() != null
				&& dp.d().getPlace().getLocation() != null) {
			dp.d().getPlace().getLocation().setTimestamp(dp.d().getEndTime());
			emitTrackPoint(dp.d().getPlace().getLocation(), dp.d(), collector);
		}
	}

	@Override
	public void finishWindow(TimeWindow window, BasicOutputCollector collector) {
		// TODO Auto-generated method stub

	}

	@Override
	public void snapshotWindow(TimeWindow window, BasicOutputCollector collector) {
		// TODO Auto-generated method stub

	}

}
