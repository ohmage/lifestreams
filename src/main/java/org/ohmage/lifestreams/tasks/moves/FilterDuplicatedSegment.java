package org.ohmage.lifestreams.tasks.moves;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.joda.time.Days;
import org.ohmage.lifestreams.models.StreamRecord;
import org.ohmage.lifestreams.tasks.SimpleTimeWindowTask;
import org.ohmage.lifestreams.tasks.TimeWindow;
import org.springframework.stereotype.Component;

import co.nutrino.api.moves.impl.dto.storyline.MovesSegment;
@Component
public class FilterDuplicatedSegment extends SimpleTimeWindowTask<MovesSegment>{
	public FilterDuplicatedSegment() {
		super(Days.ONE);
	}
	
	MovesSegment lastSegment;
	@Override
	public void executeDataPoint(StreamRecord<MovesSegment> record,
			TimeWindow window) {
		if(lastSegment != null){
			if(!lastSegment.getStartTime().equals(record.getData().getStartTime())){
				// we may receive consecutive segments with the same start time
				// so we only emit a segment if its following segment has a different start time
				this.createRecord()
					.setData(lastSegment)
					.setTimestamp(lastSegment.getEndTime())
					.emit();;
			}
		}
		lastSegment = record.getData();
		checkpoint(record.getTimestamp());
	}
 
	@Override
	public void finishWindow(TimeWindow window) {

	}
}
