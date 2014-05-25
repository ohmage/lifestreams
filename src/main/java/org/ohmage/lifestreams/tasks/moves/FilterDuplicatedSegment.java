package org.ohmage.lifestreams.tasks.moves;

import org.ohmage.lifestreams.models.StreamRecord;
import org.ohmage.lifestreams.tasks.SimpleTask;
import org.springframework.stereotype.Component;

import co.nutrino.api.moves.impl.dto.storyline.MovesSegment;
@Component
public class FilterDuplicatedSegment extends SimpleTask<MovesSegment>{
	MovesSegment lastSegment;
	@Override
	public void executeDataPoint(StreamRecord<MovesSegment> record) {
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
 

}
