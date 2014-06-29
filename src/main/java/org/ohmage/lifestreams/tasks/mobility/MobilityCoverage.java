package org.ohmage.lifestreams.tasks.mobility;

import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.base.BaseSingleFieldPeriod;
import org.ohmage.lifestreams.models.StreamRecord;
import org.ohmage.lifestreams.models.data.DataCoverage;
import org.ohmage.lifestreams.models.data.MobilityData;
import org.ohmage.lifestreams.tasks.SimpleTimeWindowTask;
import org.ohmage.lifestreams.tasks.TimeWindow;

/**
 * MobilityCoverage compute the coverage of mobility data in each time window
 * <p/>
 * Created by changun on 6/20/14.
 */
public class MobilityCoverage extends SimpleTimeWindowTask<MobilityData> {
    public MobilityCoverage(BaseSingleFieldPeriod period) {
        super(period);
    }

    DateTime lastTime;
    long millis = 0;

    @Override
    public void executeDataPoint(StreamRecord<MobilityData> record, TimeWindow window) {
        if (lastTime != null) {
            long duration = new Duration(lastTime, record.getTimestamp()).getMillis();
            if (duration < 7 * 60 * 1000) {
                millis += duration;
            }
        }
        lastTime = record.getTimestamp();
    }

    @Override
    public void finishWindow(TimeWindow window) {
        Double coverage = ((double) millis) / window.getTimeWindowSizeInMillis();
        DataCoverage data = new DataCoverage(window.getTimeWindowBeginTime(), window.getTimeWindowEndTime(), coverage);
        this.createRecord()
                .setTimestamp(window.getTimeWindowBeginTime())
                .setData(data)
                .emit();
        millis = 0;
        checkpoint(window.getTimeWindowEndTime());
    }
}
