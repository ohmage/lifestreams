package org.ohmage.lifestreams.test.activityCount;

import org.joda.time.Days;
import org.ohmage.lifestreams.models.StreamRecord;
import org.ohmage.lifestreams.models.data.MobilityData;
import org.ohmage.lifestreams.tasks.SimpleTimeWindowTask;
import org.ohmage.lifestreams.tasks.TimeWindow;
import org.springframework.stereotype.Component;

/**
 * @author changun This example counts the number of activity instances in the
 *         mobility data of each time window, and outputs the
 *         ActivityInstanceCountData.        
 */
@Component											
public class ActivityInstanceCounter extends SimpleTimeWindowTask<MobilityData>{ // input data type = MobilityData
	public ActivityInstanceCounter() {
		super(Days.ONE);
	}

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
				.setData(data)
				.emit();
		// restart the counter
		activityInstanceCount = 0;
		checkpoint(window.getTimeWindowEndTime());
		
	}

		
	
}
