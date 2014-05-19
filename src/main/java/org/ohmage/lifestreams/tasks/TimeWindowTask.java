package org.ohmage.lifestreams.tasks;

import java.util.ArrayList;
import java.util.List;

import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.base.BaseSingleFieldPeriod;
import org.ohmage.lifestreams.bolts.LifestreamsBolt;
import org.ohmage.lifestreams.models.StreamRecord;
import org.ohmage.lifestreams.spouts.RedisBookkeeper;
import org.ohmage.lifestreams.tasks.Task.RecordBuilder;
import org.ohmage.lifestreams.tuples.RecordTuple;
import org.ohmage.lifestreams.utils.PendingBuffer;
import org.ohmage.models.OhmageUser;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import backtype.storm.generated.GlobalStreamId;

/**
 * @author changun
 * 
 *         SimpleTask is the recommended way to implement a Lifestreams module.
 *         Each simple task instance performs a certain computation for one
 *         single user (i.e. it is guaranteed that the same simple task instance
 *         will only receive, and is the only one to receive, the same user's
 *         data. ). The computation is assumed to be performed on a fixed-sized
 *         time window (e.g. the "daily" geo-diameter, the weekly moving
 *         patterns etc.) The The Lifestreams framework will maintain a time
 *         window for each SimpleTask (see BasicLifestreamsBolt, and
 *         TimeWindowBolt).
 * 
 *         The init() method will be called when the task instance just created.
 *         It is the place where you can initialize the state of the
 *         computation.
 * 
 *         The executeDataPoint() will be called when receiving a new record in the
 *         current time window. It is typical to update the computation state
 *         with the new data point in this method.
 * 
 *         The finishWindow() method will be called when we have received all
 *         the records for the current time window. It is typical to finalize
 *         the computation, emit the output record, and re-initialize the
 *         computation states for the next time window in this method.
 * 
 *         In addition, the snapshotWindow() method will be called when we
 *         receive a "snapshot" command, which usually happens when the
 *         front-end applications need the partial computation results ASAP even
 *         before receiving all the data for the current time window from the
 *         user. A typical snapshotWindow() method would perform the same
 *         computation as in the finishWindow() method, but without
 *         reinitializing the computation states. emit() and emitSnapshot()
 *         method can be used to emit the output record to the next nodes in the
 *         topology or writeback to ohmage (if the target stream is specified when
 *         creating the topology).
 * @param <INPUT>
 *            the data type the task expects to receive.
 */
public abstract class TimeWindowTask extends Task {

	transient private TimeWindow curTimeWindow;
	transient private PendingBuffer pendingBuf;
	private BaseSingleFieldPeriod timeWindowSize;

	
	public TimeWindowTask(BaseSingleFieldPeriod timeWindowSize){
		this.timeWindowSize = timeWindowSize;
	}
	public TimeWindowTask(){
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
	public void executeDataPoint(RecordTuple tuple){
		StreamRecord rec = tuple.getStreamRecord();
		// init the cur timewindow if it has not been initialized
		if(curTimeWindow == null){
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
			if (pendingBuf.getPendingStreams().size() == getState().getBolt().getSourceIds().size()) {
				// when the number of unique pending stream == the number of source streams
				// it means that every input stream has at least one data point pending.
				// Then, we assume we have received all the data for the current time window
				
				// finalize the computation for the current time window
				finishWindow(curTimeWindow);
				// clear the time window, so we can move on to the next one
				curTimeWindow = null;
				// take all the points out from the pending buffer (they are sorted by time)
				List<RecordTuple> pendings = new ArrayList<RecordTuple>(pendingBuf.getBuffer());
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
