package org.ohmage.lifestreams.tasks;

import org.ohmage.lifestreams.models.StreamRecord;
import org.ohmage.lifestreams.tuples.RecordTuple;

/**
 * 
 * A simplied version of Task which assumes that the input is always a stream
 * record with data of type T
 * 
 * @author changun
 * 
 * @param <T>
 *            the type of the data that input stream record contains
 */
public abstract class SimpleTask<T> extends Task {

	protected RecordBuilder createRecord() {
		return new RecordBuilder();
	}

	@Override
	public void executeDataPoint(RecordTuple tuple) {
		executeDataPoint(tuple.getStreamRecord());
	}

	@Override
    protected void init() {
		super.init();
		if (this.getState().getBolt().getInputStreams().size() != 1) {
			throw new RuntimeException(
					"SimpleTasks are only allowed to have one source stream.");
		}
	}

	abstract public void executeDataPoint(StreamRecord<T> record);
}
