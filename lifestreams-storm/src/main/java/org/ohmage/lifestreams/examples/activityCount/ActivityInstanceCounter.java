package org.ohmage.lifestreams.examples.activityCount;

import org.ohmage.lifestreams.bolts.TimeWindow;
import org.ohmage.lifestreams.models.StreamRecord;
import org.ohmage.lifestreams.models.data.MobilityData;
import org.ohmage.lifestreams.tasks.SimpleTask;

/**
 * @author changun This example counts the number of activity instances in the
 *         mobility data of each time window, and outputs the
 *         ActivityInstanceCountData.        
 */
														
public class ActivityInstanceCounter extends SimpleTask<MobilityData>{ // input data type = MobilityData
	// counter for activity instances
	int activityInstanceCount = 0;
	@Override
	public void executeDataPoint(StreamRecord<MobilityData> rec,
			TimeWindow window) {
		if(rec.getData().getMode().isActive()){
			// increment the counter if the mobility state of this data point is active
			activityInstanceCount ++;
		}
		
	}

	@Override
	public void finishWindow(TimeWindow window) {
		// create activity instance count data point and emit the record
		ActivityInstanceCountData data = new ActivityInstanceCountData(window, this);
		this.createRecord()
				.setTimestamp(window.getFirstInstant())
				.setIsSnapshot(false)
				.setData(data)
				.emit();
		// restart the counter
		activityInstanceCount = 0;
		
	}

	@Override
	public void snapshotWindow(TimeWindow window) {
		// do the same thing as in finishWindow, but without restarting the counter
		ActivityInstanceCountData data = new ActivityInstanceCountData(window, this);
		this.createRecord()
				.setTimestamp(window.getFirstInstant())
				.setIsSnapshot(true)
				.setData(data)
				.emit();
	}
		
	
}
