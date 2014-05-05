package org.ohmage.lifestreams.tasks;

import org.ohmage.lifestreams.models.StreamRecord;

import backtype.storm.generated.GlobalStreamId;

public abstract class SimpleTask<T> extends Task {

	@Override
	public void executeDataPoint(StreamRecord record, GlobalStreamId source) {
		executeDataPoint(record);

	}
	abstract public void executeDataPoint(StreamRecord<T> record);
}
