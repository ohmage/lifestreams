package org.ohmage.lifestreams.models.data;

import org.ohmage.lifestreams.bolts.IGenerator;
import org.ohmage.lifestreams.tasks.TimeWindow;

public class TimeWindowData extends LifestreamsData {
    private TimeWindow timeWindow;

    public TimeWindow getTimeWindow() {
        return timeWindow;
    }

    public TimeWindowData(TimeWindow window, IGenerator generator) {
        super(generator);
        // populate the timewindow this data point cover
        this.timeWindow = window;
    }

}
