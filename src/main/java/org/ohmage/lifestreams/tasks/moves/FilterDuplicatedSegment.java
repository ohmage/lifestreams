package org.ohmage.lifestreams.tasks.moves;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;
import org.ohmage.lifestreams.bolts.TimeWindow;
import org.ohmage.lifestreams.models.StreamRecord;
import org.ohmage.lifestreams.tasks.SimpleTask;
import org.springframework.stereotype.Component;

import co.nutrino.api.moves.impl.dto.storyline.MovesSegment;
@Component
public class FilterDuplicatedSegment extends SimpleTask<MovesSegment>{
	Map<Long, MovesSegment> map = new HashMap<Long, MovesSegment>();
	@Override
	public void executeDataPoint(StreamRecord<MovesSegment> record,
			TimeWindow window) {
		map.put(record.d().getEndTime().getMillis(), record.d());
		
	}

	@Override
	public void finishWindow(TimeWindow window) {
		List<Long> segmentTimes = new ArrayList<Long>(map.keySet());
		Collections.sort(segmentTimes);
		for(Long segmentTime: segmentTimes){
			MovesSegment segment = map.get(segmentTime);
			this.createRecord().setData(segment).setTimestamp(segment.getEndTime()).emit();;
		}
		map.clear();
		
	}

	@Override
	public void snapshotWindow(TimeWindow window) {
		List<Long> segmentTimes = new ArrayList<Long>(map.keySet());
		Collections.sort(segmentTimes);
		for(Long segmentTime: segmentTimes){
			MovesSegment segment = map.get(segmentTime);
			this.createRecord().setData(segment).setTimestamp(segment.getEndTime()).setIsSnapshot(true).emit();;
		}
	}

}
