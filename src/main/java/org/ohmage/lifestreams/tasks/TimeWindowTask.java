package org.ohmage.lifestreams.tasks;

import java.util.ArrayList;
import java.util.List;

import org.joda.time.Days;
import org.joda.time.base.BaseSingleFieldPeriod;
import org.ohmage.lifestreams.models.StreamRecord;
import org.ohmage.lifestreams.tuples.RecordTuple;
import org.ohmage.lifestreams.utils.PendingBuffer;

/**
 * TimeWindowTask provides additional function for the tasks that aggregate the
 * data in the unit of time windows. In additon to
 * {@link #executeDataPoint(RecordTuple)} method that is called when receiving a
 * data pointa, {@link #finishWindow(TimeWindow)} method is called when all the
 * data in the current time window has been received.
 */
public abstract class TimeWindowTask extends Task {

	transient private TimeWindow curTimeWindow;
	transient private PendingBuffer pendingBuf;
	private BaseSingleFieldPeriod timeWindowSize;

	public TimeWindowTask(BaseSingleFieldPeriod timeWindowSize) {
		this.timeWindowSize = timeWindowSize;
	}

	public TimeWindowTask() {
		this(Days.ONE);
	}

	public void setTimeWindowSize(BaseSingleFieldPeriod timeWindowSize) {
		this.timeWindowSize = timeWindowSize;
	}

	@Override
	public void init() {
		super.init();
		pendingBuf = new PendingBuffer();
	}

	@Override
	public void recover() {
		super.recover();
		pendingBuf = new PendingBuffer();
	}
	@Override
	protected void checkpoint(){
		throw new UnsupportedOperationException("You  must commit a checkpoint with timestamp.");
	}
	@Override
	public void executeDataPoint(RecordTuple tuple) {
		StreamRecord rec = tuple.getStreamRecord();
		// init the cur timewindow if it has not been initialized
		if (curTimeWindow == null) {
			curTimeWindow = new TimeWindow(timeWindowSize, rec.getTimestamp());
		}

		// check if the received record fall into the current time window
		if (curTimeWindow.withinWindow(rec.getTimestamp())) {
			// if so, update the time window statistics
			curTimeWindow.update(rec.getTimestamp());
			// and process the record
			executeDataPoint(tuple, curTimeWindow);

		} else {
			// if not so, store that record to the pending buffer
			pendingBuf.put(tuple);
			if (pendingBuf.getPendingStreams().size() == getState().getBolt()
					.getSourceIds().size()) {
				// when the number of unique pending stream == the number of
				// source streams
				// it means that every input stream has at least one data point
				// pending.
				// Then, we assume we have received all the data for the current
				// time window

				// finalize the computation for the current time window
				finishWindow(curTimeWindow);
				// clear the time window, so we can move on to the next one
				curTimeWindow = null;
				// take all the points out from the pending buffer (they are
				// sorted by time)
				List<RecordTuple> pendings = new ArrayList<RecordTuple>(
						pendingBuf.getBuffer());
				pendingBuf.clearBuffer();
				// replay all the pending record
				for (RecordTuple pendingTuple : pendings) {
					executeDataPoint(pendingTuple);
				}
			}

		}
	}

	public abstract void executeDataPoint(RecordTuple input, TimeWindow window);

	public abstract void finishWindow(TimeWindow window);

}
