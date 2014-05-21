package org.ohmage.lifestreams.test.activityCount;

import org.joda.time.Days;
import org.ohmage.lifestreams.models.StreamRecord;
import org.ohmage.lifestreams.models.data.MobilityData;
import org.ohmage.lifestreams.tasks.SimpleTimeWindowTask;
import org.ohmage.lifestreams.tasks.TimeWindow;
import org.springframework.stereotype.Component;

/**
 * This example counts the number of active (walking/running/cycling) instances
 * in a day based on mobility data , and outputs the ActivityInstanceCountData.
 * 
 * @author changun
 */
@Component
public class ActivityInstanceCounter extends SimpleTimeWindowTask<MobilityData> { 
	
	public ActivityInstanceCounter() {
		//  by default, count the activity instances for each day.
		super(Days.ONE);
	}

	// counter for active instances
	int activityInstanceCount = 0;

	@Override
	public void executeDataPoint(StreamRecord<MobilityData> rec,
			TimeWindow window) {
		if (rec.getData().getMode().isActive()) {
			// increment the counter if the mobility state is active (walking/running/cycling)
			activityInstanceCount++;
		}

	}

	@Override
	public void finishWindow(TimeWindow window) {
		// create data point for active instances count and emit the record
		ActivityInstanceCountData data = new ActivityInstanceCountData(window,
				this);
		this.createRecord()
				.setTimestamp(window.getTimeWindowBeginTime()) // use begin time of the time window as the timestamp
				.setData(data)
				.emit();
		// clear the counter
		activityInstanceCount = 0;
		// commit a checkpoint
		checkpoint(window.getTimeWindowEndTime());

	}

}
