package lifestreams.bolt;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.joda.time.base.BaseSingleFieldPeriod;

import state.UserState;
import lifestreams.model.StreamRecord;

import org.ohmage.models.OhmageStream;
import org.ohmage.models.OhmageUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lifestreams.utils.PendingBuffer;
import lifestreams.utils.PendingBuffer.RecordAndSource;
import lifestreams.utils.TimeWindow;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;

public abstract class LifestreamsBolt extends BaseLifestreamsBolt {
	Logger logger = LoggerFactory.getLogger(this.getClass());

	private static final String PENDING_BUFFER = "PENDING_BUFFER";

	private static final String CUR_TIME_WINDOW = "CUR_TIME_WINDOW";

	// the duration of the time window
	BaseSingleFieldPeriod timeWindowSize;

	// the operation to perform upon receiving each incoming data point.
	protected abstract boolean executeDataPoint(OhmageUser user,
			StreamRecord dp, UserState state, TimeWindow window,
			BasicOutputCollector collector);

	// the operation to perform upon receiving all the data points in the
	// current time window
	protected abstract void finishWindow(OhmageUser user, UserState state,
			TimeWindow window, BasicOutputCollector collector);

	// the operation to perform when receive snapshot signal
	abstract protected void snapshotWindow(OhmageUser user, UserState state,
			TimeWindow window, BasicOutputCollector collector);

	@Override
	protected void newUser(OhmageUser user, UserState state) {
		PendingBuffer pendingBuf = new PendingBuffer();
		state.put(PENDING_BUFFER, pendingBuf);
	}

	@Override
	public void execute(OhmageUser user, StreamRecord dp, UserState state,
			BasicOutputCollector collector, GlobalStreamId source) {
		TimeWindow curTimeWindow;
		if (state.containsKey(CUR_TIME_WINDOW)) {
			curTimeWindow = (TimeWindow) state.get(CUR_TIME_WINDOW);
		} else {
			curTimeWindow = new TimeWindow(timeWindowSize, dp.getTimestamp());
			state.put(CUR_TIME_WINDOW, curTimeWindow);
		}

		if (curTimeWindow.withinWindow(dp.getTimestamp())) {
			// time window object will keep some stats for all the point
			// received
			curTimeWindow.update(dp.getTimestamp());
			executeDataPoint(user, dp, state, curTimeWindow, collector);
		} else {
			PendingBuffer pendingBuf = (PendingBuffer) state
					.get(PENDING_BUFFER);
			pendingBuf.put(dp, source);

			// when every input stream has pending data point,
			// we assume we have received all the data for the window
			if (pendingBuf.getPendingStreams().size() == this.inputStreams
					.size()) {
				// finalize the computation
				finishWindow(user, state, curTimeWindow, collector);

				// take all the points out from the pending buffer
				List<RecordAndSource> pendings = new ArrayList<RecordAndSource>(
						pendingBuf.getBuffer());
				pendingBuf.clearBuffer();

				// update the current time window as the time of next dp
				StreamRecord nextDp = pendings.get(0).getData();
				curTimeWindow = new TimeWindow(timeWindowSize,
						nextDp.getTimestamp());
				state.put(CUR_TIME_WINDOW, curTimeWindow);

				// replay all the pending dp
				for (RecordAndSource dataAndSource : pendings) {
					execute(user, dataAndSource.getData(), state, collector,
							dataAndSource.getSource());
				}

			}

		}
	}

	// the operation to perform when receive snapshot signal
	@Override
	protected void executeCommand(OhmageUser user, Command command,
			UserState state, BasicOutputCollector collector) {
		if (command.equals(Command.CommandType.SNAPSHOT)) {
			snapshotWindow(user, state,
					(TimeWindow) state.get(CUR_TIME_WINDOW), collector);
		}
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		super.prepare(stormConf, context);
	}

	public LifestreamsBolt(BaseSingleFieldPeriod timeWindowSize) {
		this.timeWindowSize = timeWindowSize;
	}

	public LifestreamsBolt(BaseSingleFieldPeriod timeWindowSize,
			OhmageStream targetStream) {
		super(targetStream);
		this.timeWindowSize = timeWindowSize;
	}
}
