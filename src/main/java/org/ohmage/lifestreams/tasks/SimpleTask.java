package org.ohmage.lifestreams.tasks;

import org.ohmage.lifestreams.bolts.LifestreamsBolt;
import org.ohmage.lifestreams.models.StreamRecord;
import org.ohmage.models.OhmageUser;

import backtype.storm.generated.GlobalStreamId;

public abstract class SimpleTask<T> extends Task {

	@Override
	public void executeDataPoint(StreamRecord record, GlobalStreamId source) {
		executeDataPoint(record);

	}
	@Override
	public void init(OhmageUser user, LifestreamsBolt bolt) {
		super.init(user, bolt);
		if(this.getBolt().getInputStreams().size() != 1){
			throw new RuntimeException("SimpleTasks are only allowed to have one source stream.");
		}
	}
	abstract public void executeDataPoint(StreamRecord<T> record);
}
