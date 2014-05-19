package org.ohmage.lifestreams.tasks;

import org.ohmage.lifestreams.bolts.LifestreamsBolt;
import org.ohmage.lifestreams.models.StreamRecord;
import org.ohmage.lifestreams.tasks.Task.RecordBuilder;
import org.ohmage.lifestreams.tuples.RecordTuple;
import org.ohmage.models.OhmageUser;

import backtype.storm.generated.GlobalStreamId;

public abstract class SimpleTask<T> extends Task {

	protected RecordBuilder createRecord(){
		return new RecordBuilder();
	}
	@Override
	public void executeDataPoint(RecordTuple tuple) {
		executeDataPoint(tuple.getStreamRecord());
	}
	@Override
	public void init() {
		super.init();
		if(this.getState().getBolt().getInputStreams().size() != 1){
			throw new RuntimeException("SimpleTasks are only allowed to have one source stream.");
		}
	}
	abstract public void executeDataPoint(StreamRecord<T> record);
}
