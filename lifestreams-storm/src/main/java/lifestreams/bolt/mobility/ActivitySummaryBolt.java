package lifestreams.bolt.mobility;

import java.util.ArrayList;
import java.util.EnumMap;

import lifestreams.bolt.LifestreamsBolt;
import lifestreams.model.MobilityState;
import lifestreams.model.StreamRecord;
import lifestreams.model.data.ActivityInstance;
import lifestreams.model.data.ActivitySummaryData;
import lifestreams.model.data.IMobilityData;
import org.ohmage.models.OhmageStream;
import org.ohmage.models.OhmageUser;

import lifestreams.utils.ActivityInstanceAccumulator;
import lifestreams.utils.TimeWindow;

import org.joda.time.DateTime;
import org.joda.time.base.BaseSingleFieldPeriod;

import state.UserState;
import backtype.storm.topology.BasicOutputCollector;

public class ActivitySummaryBolt extends LifestreamsBolt {

	private static final String CURRENT_ACTIVITY_INSTANCE_ACCUMULATOR = "CURRENT_ACTIVITY_INSTANCE_ACCUMULATOR";
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final String LAST_DATA_POINT = "LAST_DATA_POINT";
	private static final String ACCUMULATED_TIME = "ACCUMULATED_TIME";
	private static final String ACTIVITY_INSTANCES = "ACTIVITY_INSTANCES";

	private static final Double LONGEST_SAMPLING_PERIOD = 5.5 * 60 * 1000; // in
																			// millisec

	public ActivitySummaryBolt(BaseSingleFieldPeriod period) {
		super(period);
	}

	public ActivitySummaryBolt(BaseSingleFieldPeriod period, OhmageStream target) {
		super(period, target);
	}

	@Override
	protected void newUser(OhmageUser user, UserState state) {
		super.newUser(user, state);
		EnumMap<MobilityState, Double> accumulatedTime = new EnumMap<MobilityState, Double>(
				MobilityState.class);
		for (MobilityState mState : MobilityState.values()) {
			accumulatedTime.put(mState, 0.0);
		}
		state.put(ACCUMULATED_TIME, accumulatedTime);
		state.put(ACTIVITY_INSTANCES, new ArrayList<ActivityInstance>());
	}

	@SuppressWarnings("unchecked")
	public EnumMap<MobilityState, Double> getAccumulatedTime(UserState state) {
		return (EnumMap<MobilityState, Double>) state.get(ACCUMULATED_TIME);
	}

	@SuppressWarnings("unchecked")
	public ArrayList<ActivityInstance> getActivityInstances(UserState state) {
		return (ArrayList<ActivityInstance>) state.get(ACTIVITY_INSTANCES);
	}

	public ActivityInstanceAccumulator getCurrentActivityInstanceAccumulator(
			UserState state) {
		return (ActivityInstanceAccumulator) state
				.get(CURRENT_ACTIVITY_INSTANCE_ACCUMULATOR);
	}

	public void setCurrentActivityInstanceAccumulator(UserState state,
			ActivityInstanceAccumulator gen) {
		state.put(CURRENT_ACTIVITY_INSTANCE_ACCUMULATOR, gen);
	}

	public StreamRecord<IMobilityData> getLastRecord(UserState state) {
		return (StreamRecord<IMobilityData>) state.get(LAST_DATA_POINT);
	}

	private void accumulateActivityTimes(UserState state,
			StreamRecord<IMobilityData> cur_dp) {
		StreamRecord<IMobilityData> last_dp = getLastRecord(state);
		if (last_dp != null) {
			MobilityState curState = cur_dp.d().getMode();
			MobilityState prevState = last_dp.d().getMode();
			DateTime dt = cur_dp.getTimestamp();
			// get duration in seconds
			long duration = (dt.getMillis() - last_dp.getTimestamp()
					.getMillis()) / 1000;
			// only accumulate the samples that with sufficient frequency
			if (duration < LONGEST_SAMPLING_PERIOD) {
				Double halfPeriod = duration / 2.0;
				EnumMap<MobilityState, Double> accumulatedTime = getAccumulatedTime(state);
				// both state the sandwiches this duration get a half of the
				// duration
				accumulatedTime.put(prevState, accumulatedTime.get(prevState)
						+ halfPeriod);
				accumulatedTime.put(curState, accumulatedTime.get(curState)
						+ halfPeriod);
			}
		}
	}

	private void accumulateActivityInstance(UserState state,
			StreamRecord<IMobilityData> cur_dp) {
		StreamRecord<IMobilityData> last_dp = getLastRecord(state);
		if (cur_dp.d().getMode().isActive()) { // if the current state is active
			// add this point to the accumulator
			if (getCurrentActivityInstanceAccumulator(state) == null)
				setCurrentActivityInstanceAccumulator(state,
						new ActivityInstanceAccumulator());
			getCurrentActivityInstanceAccumulator(state).addDataPoint(cur_dp);
		} else if (last_dp != null && last_dp.d().getMode().isActive()) { // if
																			// the
																			// last
																			// state
																			// is
																			// active
			// get the accumulated activity instance til the last data point
			ActivityInstance instance = getCurrentActivityInstanceAccumulator(
					state).getInstance();
			if (instance.getDistance() != 0.0) {
				// add that to the activity instances array
				getActivityInstances(state).add(instance);
			}
			// clear the current accumulator
			setCurrentActivityInstanceAccumulator(state, null);
		}

	}

	private void updateSummary(UserState state,
			StreamRecord<IMobilityData> cur_dp) {
		/* Task 1. accumulate total time for each type of activity */
		accumulateActivityTimes(state, cur_dp);

		/* Task 2. accumulate the activity instances */

		// an activity instance is composed of a series of consecutive active
		// state
		// an activity instance accumulator will compute the statisitcs (e.g.
		// duration, distance) for that instance
		accumulateActivityInstance(state, cur_dp);
	}

	@Override
	protected boolean executeDataPoint(OhmageUser user, StreamRecord dp,
			UserState state, TimeWindow window, BasicOutputCollector collector) {
		// check if we go back in time
		StreamRecord<IMobilityData> cur_dp = dp;
		StreamRecord<IMobilityData> prev_dp = getLastRecord(state);
		if (prev_dp == null
				|| cur_dp.getTimestamp().isAfter(prev_dp.getTimestamp()))
			updateSummary(state, cur_dp);

		state.put(LAST_DATA_POINT, cur_dp);
		return false;
	}

	private StreamRecord<ActivitySummaryData> computeSummaryDataPoint(
			OhmageUser user, UserState state, TimeWindow window) {
		// check if there is an activity instance still being accumulated
		if (getCurrentActivityInstanceAccumulator(state) != null) {
			// get the accumulated activity instance
			ActivityInstance instance = getCurrentActivityInstanceAccumulator(
					state).getInstance();
			// add that to the instance array
			getActivityInstances(state).add(instance);
		}
		double totalActiveTime = getAccumulatedTime(state).get(
				MobilityState.WALK)
				+ getAccumulatedTime(state).get(MobilityState.RUN)
				+ getAccumulatedTime(state).get(MobilityState.CYCLING);

		double totalSedentaryTime = getAccumulatedTime(state).get(
				MobilityState.DRIVE)
				+ getAccumulatedTime(state).get(MobilityState.STILL);

		double totalTime = totalActiveTime + totalSedentaryTime;
		double totalTransportationTime = getAccumulatedTime(state).get(
				MobilityState.DRIVE);

		ActivitySummaryData data = new ActivitySummaryData(window, this)
				.setTotalActiveTime(totalActiveTime)
				.setTotalSedentaryTime(totalSedentaryTime)
				.setTotalTime(totalTime)
				.setTotalTransportationTime(totalTransportationTime)
				.setActivityInstances(getActivityInstances(state));
		StreamRecord<ActivitySummaryData> outputRecord = new StreamRecord<ActivitySummaryData>(
				user, window.getLastInstant());
		outputRecord.setData(data);
		return outputRecord;
	}

	private void cleanUp(UserState state) {
		state.remove(LAST_DATA_POINT);
		state.remove(CURRENT_ACTIVITY_INSTANCE_ACCUMULATOR);
		EnumMap<MobilityState, Double> accumulatedTime = getAccumulatedTime(state);
		for (MobilityState mState : MobilityState.values()) {
			accumulatedTime.put(mState, 0.0);
		}
		getActivityInstances(state).clear();
	}

	@Override
	protected void finishWindow(OhmageUser user, UserState state,
			TimeWindow window, BasicOutputCollector collector) {
		this.emit(computeSummaryDataPoint(user, state, window), collector);
		cleanUp(state);
	}

	@Override
	protected void snapshotWindow(OhmageUser user, UserState state,
			TimeWindow window, BasicOutputCollector collector) {

		this.emitSnapshot(computeSummaryDataPoint(user, state, window),
				collector);

	}

}
