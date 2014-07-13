package org.ohmage.lifestreams.tasks.moves;

import co.nutrino.api.moves.impl.dto.storyline.MovesSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.ohmage.lifestreams.models.StreamRecord;
import org.ohmage.lifestreams.tasks.SimpleTask;
import org.springframework.stereotype.Component;

@Component
public class FilterDuplicatedSegment extends SimpleTask<MovesSegment> {
    private MovesSegment lastSegment;
    private DateTime emittedUtil;
    @Override
    public void executeDataPoint(StreamRecord<MovesSegment> record) {
        MovesSegment curSegment = record.getData();
        if (lastSegment != null) {

            Interval lastInterval = new Interval(lastSegment.getStartTime(), lastSegment.getEndTime());
            Interval curInterval = new Interval(curSegment.getStartTime(), curSegment.getEndTime());
            Interval overlap = lastInterval.overlap(curInterval);
            // check if last segment overlap with the cur segment
            if (overlap != null) {
                // if so, shorten the time span of the last segment, so that there is no overlap
                lastSegment.setEndTime(overlap.getStart());
            }
            // check if the last segment overlap with the segments that has been emmited
            if(emittedUtil != null && lastSegment.getStartTime().isBefore(emittedUtil)){
                lastSegment.setEndTime(emittedUtil);
            }
            // emit the the last segment if it still exclusively covers some time interval after shortening

            if (lastSegment.getEndTime().isAfter(lastSegment.getStartTime())) {
                this.createRecord()
                        .setData(lastSegment)
                        .setTimestamp(lastSegment.getEndTime())
                        .emit();
                emittedUtil = lastSegment.getEndTime();
            }
        }
        lastSegment = curSegment;
        checkpoint(record.getTimestamp());
    }


}
