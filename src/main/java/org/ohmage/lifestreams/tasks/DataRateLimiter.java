package org.ohmage.lifestreams.tasks;

import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.ohmage.lifestreams.tuples.RecordTuple;

public class DataRateLimiter extends Task {
	private DateTime lastEmittedTime = null;
	private Duration minInterval;
	private int tuplesSinceCheckpoint = 0;
	@Override
	protected void executeDataPoint(RecordTuple tuple) {
		boolean tooFrequent = false;
		if(lastEmittedTime != null){
			tooFrequent = new Interval(lastEmittedTime, tuple.getTimestamp()).toDuration().isShorterThan(minInterval);
		}
		if(!tooFrequent){
			this.createRecord()
					.setTimestamp(tuple.getTimestamp())
					.setLocation(tuple.getStreamRecord().getLocation())
					.setData(tuple.getStreamRecord().getData())
					.emit();
			lastEmittedTime = tuple.getTimestamp();
			tuplesSinceCheckpoint ++;
		}
		if(tuplesSinceCheckpoint >= 100){
			checkpoint();
			tuplesSinceCheckpoint = 0;
		}
		

	}
	public DataRateLimiter(Duration minimunInterval){
		this.minInterval = minimunInterval;
	}

}
