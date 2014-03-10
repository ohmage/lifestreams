package lifestreams.tasks.moves;

import java.util.ArrayList;

import lifestreams.bolts.TimeWindow;
import lifestreams.models.MobilityState;
import lifestreams.models.StreamRecord;
import lifestreams.models.data.ActivityInstance;
import lifestreams.models.data.ActivitySummaryData;
import lifestreams.tasks.SimpleTask;

import org.joda.time.Interval;

import co.nutrino.api.moves.impl.dto.activity.MovesActivity;
import co.nutrino.api.moves.impl.dto.storyline.MovesSegment;

/**
 * @author changun This task generates a activity summary (see
 *         ActivitySummaryData) from the Moves segments in a time window
 *         (e.g. daily).
 * 
 */
public class ActivitySummarizer extends SimpleTask<MovesSegment> {
	ArrayList<MovesSegment> segments = new ArrayList<MovesSegment>();

	@Override
	public void executeDataPoint(StreamRecord<MovesSegment> dp,	TimeWindow window) {
		// srore the Moves segment in the buffer
		segments.add(dp.d());
	}

	// this function computes the activity summary from the Moves segment we have received so far.
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
	public void finishWindow(TimeWindow window) {
		this.emit(computeSummary(window));
		segments.clear();
	}

	@Override
	public void snapshotWindow(TimeWindow window) {
		this.emitSnapshot(computeSummary(window));

	}

}
