package lifestreams.bolt.moves;

import java.util.ArrayList;

import org.joda.time.Interval;

import backtype.storm.topology.BasicOutputCollector;
import lifestreams.bolt.SimpleTask;
import lifestreams.model.MobilityState;
import lifestreams.model.StreamRecord;
import lifestreams.model.data.ActivityInstance;
import lifestreams.model.data.ActivitySummaryData;
import lifestreams.utils.TimeWindow;
import co.nutrino.api.moves.impl.dto.activity.MovesActivity;
import co.nutrino.api.moves.impl.dto.storyline.MovesSegment;

public class ActivitySummarizer extends SimpleTask<MovesSegment> {
	ArrayList<MovesSegment> segments = new ArrayList<MovesSegment>();

	@Override
	public void executeDataPoint(StreamRecord<MovesSegment> dp,
			TimeWindow window, BasicOutputCollector collector) {
		segments.add(dp.d());
	}

	private StreamRecord<ActivitySummaryData> computeSummary(TimeWindow window) {
		// check if there is an activity instance still being accumulated

		double totalActiveTime = 0;
		double totalTime = 0;
		double totalTransportationTime = 0;
		ArrayList<ActivityInstance> activityInstances = new ArrayList<ActivityInstance>();
		for (MovesSegment segment : segments) {
			// go over each segments in the time window (usually daily)

			// accumulate total time
			totalTime += new Interval(segment.getStartTime(),
					segment.getEndTime()).toDurationMillis() / 1000;
			if (segment.getActivities() != null) {
				// if there is activity in this segment
				for (MovesActivity activity : segment.getActivities()) {
					// go over each activity
					MobilityState state = MobilityState
							.fromMovesActivity(activity.getActivity());
					if (state.isActive()) {
						// accumulate active times, and generate activity
						// instance
						totalActiveTime += activity.getDuration();
						// create activity instance
						activityInstances.add(ActivityInstance
								.forMovesActivity(activity));
					} else {
						if (state.equals(MobilityState.DRIVE)) {
							// accumulate transport time
							totalTransportationTime += activity.getDuration();
						}
					}
				}
			}
		}

		ActivitySummaryData data = new ActivitySummaryData(window, this)
				.setTotalActiveTime(totalActiveTime)
				.setTotalSedentaryTime(totalTime - totalActiveTime)
				.setTotalTime(totalTime)
				.setTotalTransportationTime(totalTransportationTime)
				.setActivityInstances(activityInstances);
		StreamRecord<ActivitySummaryData> outputRecord = new StreamRecord<ActivitySummaryData>(
				getUser(), window.getLastInstant());
		outputRecord.setData(data);
		return outputRecord;
	}

	@Override
	public void finishWindow(TimeWindow window, BasicOutputCollector collector) {
		this.emit(computeSummary(window), collector);
		segments.clear();
	}

	@Override
	public void snapshotWindow(TimeWindow window, BasicOutputCollector collector) {
		this.emitSnapshot(computeSummary(window), collector);

	}

}
