package org.ohmage.lifestreams.bolts;

import java.util.ArrayList;
import java.util.List;

import org.joda.time.DateTime;
import org.joda.time.base.BaseSingleFieldPeriod;
import org.ohmage.lifestreams.models.StreamRecord;
import org.ohmage.lifestreams.models.data.LifestreamsData;
import org.ohmage.lifestreams.state.UserState;
import org.ohmage.lifestreams.utils.PendingBuffer;
import org.ohmage.lifestreams.utils.PendingBuffer.RecordAndSource;
import org.ohmage.models.OhmageUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.tuple.Tuple;

/**
 * @author changun A TimeWindow bolt is used to compute the aggregated
 *         measurements that aggregate the data by time, such as "Daily"
 *         activity summaries or "Weekly" sleep patterns, etc. It maintains a
 *         time window with specific size and checks whether the received
 *         records fall into that time window or not. If so, it calls the
 *         executeDataPoint() method to process the record. If not so, it stores
 *         the record in a pending buffer. When every input data stream has at
 *         least one record pending, it assume all the record in the current
 *         time window has been received and invoke the finishTimeWindow()
 *         method to finalize the computation and move on to the next time
 *         window.
 */
@SuppressWarnings("rawtypes")
public abstract class TimeWindowBolt extends BaseStatefulBolt {
	Logger logger = LoggerFactory.getLogger(this.getClass());

	private static final String PENDING_BUFFER = "PENDING_BUFFER";
	private static final String CUR_TIME_WINDOW = "CUR_TIME_WINDOW";
	private static final String UNANCHORED_TUPLES = "UNANCHORED_TUPLES";

	// the duration of the time window
	BaseSingleFieldPeriod timeWindowSize;
	// the state of the user that is currently being processed
	UserState curUserState;
	public BaseSingleFieldPeriod getTimeWindowSize() {
		return timeWindowSize;
	}

	public void setTimeWindowSize(BaseSingleFieldPeriod timeWindowSize) {
		this.timeWindowSize = timeWindowSize;
	}

	// the operation to perform upon receiving a record.
	protected abstract void executeDataPoint(OhmageUser user,
			StreamRecord dp, UserState state, TimeWindow window);

	// the operation to after we have received all the records in the current time window
	protected abstract void finishWindow(OhmageUser user, UserState state,
			TimeWindow window);

	// the operation to perform when receive snapshot signal
	abstract protected void snapshotWindow(OhmageUser user, UserState state,
			TimeWindow window);

	@Override
	protected void newUser(OhmageUser user, UserState state) {
		PendingBuffer pendingBuf = new PendingBuffer();
		List<Tuple> unanchoredTuples = new ArrayList<Tuple>();
		state.put(PENDING_BUFFER, pendingBuf);
		state.put(UNANCHORED_TUPLES, unanchoredTuples);
	}


	@SuppressWarnings("unchecked")
	@Override
	public void execute(OhmageUser user, Tuple input, UserState state, GlobalStreamId source) {
		curUserState = state;
		// extract the record from the tuple
		@SuppressWarnings("rawtypes")
		StreamRecord rec = (StreamRecord) input.getValueByField("datapoint");
		DateTime time = rec.getTimestamp();
		TimeWindow curTimeWindow;
		// get the current time window
		if (state.containsKey(CUR_TIME_WINDOW)) {
			curTimeWindow = (TimeWindow) state.get(CUR_TIME_WINDOW);
		} else {
			curTimeWindow = new TimeWindow(timeWindowSize, rec.getTimestamp());
			state.put(CUR_TIME_WINDOW, curTimeWindow);
		}
		
		// check if the received record fall into the current time window
		if (curTimeWindow.withinWindow(rec.getTimestamp())) {
			// if so, update the time window statistics
			curTimeWindow.update(rec.getTimestamp());
			// add the tuple to the un-anchored tuple list (these tuples will be anchored in the next emit)
			((List<Tuple>) state.get(UNANCHORED_TUPLES)).add(input);
			// and process the record
			executeDataPoint(user, rec, state, curTimeWindow);
			

			
		} else {
			// if not so, store that record to the pending buffer
			PendingBuffer pendingBuf = (PendingBuffer) state.get(PENDING_BUFFER);
			pendingBuf.put(input, time, source);
			if (pendingBuf.getPendingStreams().size() == this.inputStreams.size()) {
				// when every input stream has at least one data point pending,
				// we assume we have received all the data for the current time window
				
				// finalize the computation for the current time window
				finishWindow(user, state, curTimeWindow);
				// take all the points out from the pending buffer (they are sorted by time)
				List<RecordAndSource> pendings = new ArrayList<RecordAndSource>(pendingBuf.getBuffer());
				pendingBuf.clearBuffer();

				// update the current time window to cover the time of the first record that was in the pending buffer
				DateTime timeOfNextTimeWindow = pendings.get(0).getTime();
				curTimeWindow = new TimeWindow(timeWindowSize, timeOfNextTimeWindow);
				state.put(CUR_TIME_WINDOW, curTimeWindow);

				// replay all the pending record
				for (RecordAndSource dataAndSource : pendings) {
					execute(user, dataAndSource.getData(), state, dataAndSource.getSource());
				}
			}

		}
	}
	
	// emit a record
	public List<Integer> emit(StreamRecord<? extends Object> rec) {
		List<Tuple> unanchoredTuples = (List<Tuple>) curUserState.get(UNANCHORED_TUPLES);
		List<Integer> ret = super.emit(rec, unanchoredTuples);
		unanchoredTuples.clear();
		return ret;
	}

	// operation to perform when receiving a command
	@Override
	protected void executeCommand(OhmageUser user, Command command, UserState state) {
		if (command.equals(Command.CommandType.SNAPSHOT)) {
			// make a snapshot of the current time window when receiving a snapshot command
			snapshotWindow(user, state,	(TimeWindow) state.get(CUR_TIME_WINDOW));
		}
	}

	public TimeWindowBolt(BaseSingleFieldPeriod timeWindowSize) {
		this.timeWindowSize = timeWindowSize;
	}

}
