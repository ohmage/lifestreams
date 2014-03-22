package lifestreams.bolts;

import java.util.ArrayList;
import java.util.List;

import lifestreams.models.StreamRecord;
import lifestreams.models.data.LifestreamsData;
import lifestreams.state.UserState;
import lifestreams.utils.PendingBuffer;
import lifestreams.utils.PendingBuffer.RecordAndSource;

import org.joda.time.base.BaseSingleFieldPeriod;
import org.ohmage.models.OhmageUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.generated.GlobalStreamId;

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

	// the duration of the time window
	BaseSingleFieldPeriod timeWindowSize;

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
		state.put(PENDING_BUFFER, pendingBuf);
	}


	@Override
	public void execute(OhmageUser user, StreamRecord dp, UserState state, GlobalStreamId source) {
		TimeWindow curTimeWindow;
		
		// get the current time window
		if (state.containsKey(CUR_TIME_WINDOW)) {
			curTimeWindow = (TimeWindow) state.get(CUR_TIME_WINDOW);
		} else {
			curTimeWindow = new TimeWindow(timeWindowSize, dp.getTimestamp());
			state.put(CUR_TIME_WINDOW, curTimeWindow);
		}
		
		// check if the received record fall into the current time window
		if (curTimeWindow.withinWindow(dp.getTimestamp())) {
			// if so, update the time window statistics
			curTimeWindow.update(dp.getTimestamp());
			// and process the record
			executeDataPoint(user, dp, state, curTimeWindow);
		} else {
			// if not so, store that record to the pending buffer
			PendingBuffer pendingBuf = (PendingBuffer) state.get(PENDING_BUFFER);
			pendingBuf.put(dp, source);
			if (pendingBuf.getPendingStreams().size() == this.inputStreams.size()) {
				// when every input stream has at least one data point pending,
				// we assume we have received all the data for the current time window
				
				// finalize the computation for the current time window
				finishWindow(user, state, curTimeWindow);
				// take all the points out from the pending buffer (they are sorted by time)
				List<RecordAndSource> pendings = new ArrayList<RecordAndSource>(pendingBuf.getBuffer());
				pendingBuf.clearBuffer();

				// update the current time window to cover the time of the next record
				StreamRecord nextDp = pendings.get(0).getData();
				curTimeWindow = new TimeWindow(timeWindowSize, nextDp.getTimestamp());
				state.put(CUR_TIME_WINDOW, curTimeWindow);

				// replay all the pending record
				for (RecordAndSource dataAndSource : pendings) {
					execute(user, dataAndSource.getData(), state, dataAndSource.getSource());
				}
			}

		}
	}
	@Override
	// emit a record
	public List<Integer> emit(StreamRecord<? extends Object> rec) {
		if(rec.d() instanceof LifestreamsData){
			((LifestreamsData)rec.d()).setSnapshot(false);
		}
		
		return super.emit(rec);
	}
	// emit a snapshot record
	public List<Integer> emitSnapshot(StreamRecord<? extends Object> rec) {
		if(rec.d() instanceof LifestreamsData){
			((LifestreamsData)rec.d()).setSnapshot(true);
		}
		return super.emit(rec);
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
