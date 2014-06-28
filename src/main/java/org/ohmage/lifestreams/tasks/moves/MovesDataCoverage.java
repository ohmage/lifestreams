package org.ohmage.lifestreams.tasks.moves;

import co.nutrino.api.moves.impl.dto.storyline.MovesSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.base.BaseSingleFieldPeriod;
import org.ohmage.lifestreams.models.StreamRecord;
import org.ohmage.lifestreams.models.data.DataCoverage;
import org.ohmage.lifestreams.tasks.SimpleTask;
import org.ohmage.lifestreams.tasks.TimeWindow;

/**
 * MovesDataCoverage compute moves data coverage in each time window
 * Created by changun on 6/20/14.
 */
public class MovesDataCoverage extends SimpleTask<MovesSegment> {
    TimeWindow curWindow;
    Double curCoverage;
    BaseSingleFieldPeriod coveragePeriod;
    MovesSegment segment;
    MovesSegment lastSegment;
    public MovesDataCoverage(BaseSingleFieldPeriod coveragePeriod){
        this.coveragePeriod = coveragePeriod;
    }

    void outputCoverage(){
        try {
            DataCoverage data = new DataCoverage(
                    curWindow.getTimeWindowBeginTime(),
                    curWindow.getTimeWindowEndTime(),
                    curCoverage);
            this.createRecord()
                    .setTimestamp(curWindow.getTimeWindowBeginTime())
                    .setData(data)
                    .emit();
        }catch(Exception e){
            getLogger().error("{} last: {}-{} cur: {}-{}",
                    getUser(),
                    lastSegment.getStartTime(), lastSegment.getEndTime(),
                    segment.getStartTime(), segment.getEndTime());
            throw new RuntimeException(e);

        }


    }
    @Override
    public void executeDataPoint(StreamRecord<MovesSegment> record) {
        DateTime start = record.getData().getStartTime();
        DateTime end = record.getData().getEndTime();
        segment = record.getData();
        if (curWindow == null) {
            curWindow = new TimeWindow(coveragePeriod, start);
            curCoverage = 0.0;
        }

        while(start.isBefore(end)) {
            Interval overlap = curWindow.getTimeInterval().overlap(new Interval(start, end));
            if(overlap != null){
                double cover = overlap.toDurationMillis() / (double)curWindow.getTimeWindowSizeInMillis();
                curCoverage += cover;

                boolean intervalIsCoveredByCurWindow = curWindow.getTimeWindowEndTime().isAfter(end);
                if(intervalIsCoveredByCurWindow){
                    break;
                }else{
                    start = overlap.getEnd().plus(1);
                }
            }
            // before moving to next time window, output the current window coverage
            if(curCoverage > 0.0) {
                outputCoverage();
                curCoverage = 0.0;
                curWindow = null;

            }
            // then moves to next timeWindow
            curWindow = new TimeWindow(coveragePeriod, start);
            curCoverage = 0.0;
        }
        lastSegment = segment;
        checkpoint();
    }
}
