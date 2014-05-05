package org.ohmage.lifestreams.tasks.mobility;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;

import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.base.BaseSingleFieldPeriod;
import org.ohmage.lifestreams.bolts.LifestreamsBolt;
import org.ohmage.lifestreams.models.MobilityState;
import org.ohmage.lifestreams.models.StreamRecord;
import org.ohmage.lifestreams.models.data.ActivityEpisode;
import org.ohmage.lifestreams.models.data.ActivitySummaryData;
import org.ohmage.lifestreams.models.data.IMobilityData;
import org.ohmage.lifestreams.tasks.SimpleTimeWindowTask;
import org.ohmage.lifestreams.tasks.TimeWindow;
import org.ohmage.lifestreams.utils.ActivityEpisodeAccumulator;
import org.ohmage.models.OhmageUser;
import org.springframework.stereotype.Component;

/**
 * @author changun This task generates a activity summary (see
 *         ActivitySummaryData) from the Mobility data points in a time window
 *         (e.g. daily).
 * 
 */
@Component
public class MobilityActivitySummarizer extends SimpleTimeWindowTask<IMobilityData> {

	private static final int MAXIMUN_NON_ACTIVE_GAP = 2 * 60 * 1000; // in millisec
	private static final Double MAXIMUN_SAMPLING_GAP = 5.5 * 60; // in seconds


	EnumMap<MobilityState, Double> activityTimeAccumulator;
	ActivityEpisodeAccumulator activityEpisodeAccumulator;
	List<ActivityEpisode> activityInstances;
	StreamRecord<IMobilityData> last_dp;
	
	public void init(OhmageUser user, LifestreamsBolt bolt) {
		super.init(user, bolt);
		initAccumulators();
	}
	private void initAccumulators() {
		activityTimeAccumulator = new EnumMap<MobilityState, Double> (MobilityState.class);
		for (MobilityState mState : MobilityState.values()) {
			activityTimeAccumulator.put(mState, 0.0);
		}
		activityInstances = new ArrayList<ActivityEpisode>();
		activityEpisodeAccumulator = new ActivityEpisodeAccumulator ();
		last_dp = null;
	}
	
	private void accumulateActivityTimes(StreamRecord<IMobilityData> cur_dp) {
		if (last_dp != null) {
			MobilityState curState = cur_dp.d().getMode();
			MobilityState prevState = last_dp.d().getMode();
			DateTime dt = cur_dp.getTimestamp();
			// get duration in seconds
			long interval = (dt.getMillis() - last_dp.getTimestamp().getMillis()) / 1000;
			// only accumulate the samples with sufficient frequency
			if (interval < MAXIMUN_SAMPLING_GAP) {
				Double halfPeriod = interval / 2.0;
				// both states the sandwich the interval is responsible for one half of the duration
				activityTimeAccumulator.put(prevState, activityTimeAccumulator.get(prevState) + halfPeriod);
				activityTimeAccumulator.put(curState, activityTimeAccumulator.get(curState)	+ halfPeriod);
			}
		}
	}
	
	private void createActivityInstanceAndRestartAccumlator(){
		// get the accumulated activity instance til the last data point
		ActivityEpisode instance = activityEpisodeAccumulator.getEpisode();
		// add that to the activity instances array
		activityInstances.add(instance);
		// restart the accumulator
		activityEpisodeAccumulator = new ActivityEpisodeAccumulator ();
	}
	private void accumulateActivityInstance(StreamRecord<IMobilityData> cur_dp) {
		if (last_dp != null) {
			// check the sampling interval
			long interval = (cur_dp.getTimestamp().getMillis() - last_dp.getTimestamp().getMillis()) / 1000;
			// if the sampling interval is too long, assume the previous activity (if any) has ended
			if (interval > MAXIMUN_SAMPLING_GAP && activityEpisodeAccumulator.isInitialized()) {
				// assume the previous activity instance has ended
				createActivityInstanceAndRestartAccumlator();
				
			}
		}
		if (cur_dp.d().getMode().isActive()) { // if the current state is active
			// add this point to the accumulator
			activityEpisodeAccumulator.addDataPoint(cur_dp);
		} else if (activityEpisodeAccumulator.isInitialized() 
				&& activityEpisodeAccumulator.getEndTime().plusMillis(MAXIMUN_NON_ACTIVE_GAP).isBefore(cur_dp.getTimestamp())) { 
			// if it has been more than certain of time we have not seen an active state
			// assume the previous activity episode has ended
			createActivityInstanceAndRestartAccumlator();
		}

	}

	private void updateSummary(StreamRecord<IMobilityData> cur_dp) {
		/* Task 1. accumulate total time for the type of activity */
		accumulateActivityTimes(cur_dp);

		/* Task 2. accumulate the activity instances */

		// an activity instance is composed of continuous active data points
		// an accumulator will compute the statistics (e.g. duration, distance) for an instance
		accumulateActivityInstance(cur_dp);
	}

	@Override
	public void executeDataPoint(StreamRecord<IMobilityData> dp, TimeWindow window) {
		if (last_dp == null	|| dp.getTimestamp().isAfter(last_dp.getTimestamp())){
			// make sure we does not go back in time, then update the summaries
			updateSummary(dp);
		}
		// update the last record
		last_dp = dp;
	}

	private void computeSummaryDataPoint(TimeWindow window, boolean isSnapshot) {
		// check if there is an activity episode still being accumulated
		if (activityEpisodeAccumulator.isInitialized()) {
			// get the accumulated activity instance
			ActivityEpisode instance = activityEpisodeAccumulator.getEpisode();
			// add that to the instance array
			this.activityInstances.add(instance);
		}
		double totalActiveTime =  
				activityTimeAccumulator.get(MobilityState.WALK)
				+  activityTimeAccumulator.get(MobilityState.RUN)
				+  activityTimeAccumulator.get(MobilityState.CYCLING);

		double totalSedentaryTime =  
				activityTimeAccumulator.get(MobilityState.DRIVE)
				+  activityTimeAccumulator.get(MobilityState.STILL);

		double totalTime = totalActiveTime + totalSedentaryTime;
		double totalTransportationTime =  
				activityTimeAccumulator.get(MobilityState.DRIVE);
		double distance = 0;
		for(ActivityEpisode instance: activityInstances){
			distance += instance.getDistanceInMiles();
		}
		getLogger().info("Distance: {} miles", distance);
		ActivitySummaryData data = new ActivitySummaryData(window, this)
				.setTotalActiveTimeInSeconds(totalActiveTime)
				.setTotalSedentaryTimeInSeconds(totalSedentaryTime)
				.setTotalTimeInSeconds(totalTime)
				.setTotalTransportationTimeInSeconds(totalTransportationTime)
				.setActivityEpisodes(activityInstances);
		
		this.createRecord()
				.setData(data)
				.setTimestamp(window.getFirstInstant())
				.setIsSnapshot(isSnapshot)
				.emit();
	}


	@Override
	public void finishWindow(TimeWindow window) {
		// emit the summary
		computeSummaryDataPoint(window, false);
		// re-initialize the accumulators
		initAccumulators();
		
	}


}
