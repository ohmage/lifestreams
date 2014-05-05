package org.ohmage.lifestreams.tasks;

import org.joda.time.base.BaseSingleFieldPeriod;
import org.ohmage.lifestreams.bolts.LifestreamsBolt;
import org.ohmage.lifestreams.models.StreamRecord;
import org.ohmage.models.OhmageUser;
import org.slf4j.LoggerFactory;

import backtype.storm.generated.GlobalStreamId;

public abstract class SimpleTimeWindowTask<T> extends TimeWindowTask {

	public SimpleTimeWindowTask(){
		super();
	}
	public SimpleTimeWindowTask(BaseSingleFieldPeriod timeWindowSize) {
		super(timeWindowSize);
	}
	@Override
	public void init(OhmageUser user, LifestreamsBolt bolt) {
		super.init(user, bolt);
		if(this.getBolt().getSourceIds().size() != 1){
			throw new RuntimeException("SimpleTasks are only allowed to have one source stream.");
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void executeDataPoint(StreamRecord record, GlobalStreamId source, TimeWindow window) {
		executeDataPoint(record, window);
	}
	abstract public void executeDataPoint(StreamRecord<T> record, TimeWindow window);

	

}
