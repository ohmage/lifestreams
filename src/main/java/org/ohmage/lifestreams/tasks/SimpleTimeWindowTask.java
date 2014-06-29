package org.ohmage.lifestreams.tasks;

import org.joda.time.base.BaseSingleFieldPeriod;
import org.ohmage.lifestreams.models.StreamRecord;
import org.ohmage.lifestreams.tuples.RecordTuple;

/**
 * A simplied version of TimeWindowTask which assumes that the input is always a
 * stream record with data of type T
 *
 * @param <T> the type of the data that input stream record contains
 * @author changun
 */
public abstract class SimpleTimeWindowTask<T> extends TimeWindowTask {

    public SimpleTimeWindowTask() {
        super();
    }

    protected SimpleTimeWindowTask(BaseSingleFieldPeriod timeWindowSize) {
        super(timeWindowSize);
    }

    @Override
    public void init() {
        super.init();
        if (this.getState().getBolt().getInputStreams().size() != 1) {
            throw new RuntimeException(
                    "SimpleTasks are only allowed to have one source stream.");
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public void executeDataPoint(RecordTuple tuple, TimeWindow window) {
        executeDataPoint(tuple.getStreamRecord(), window);
    }

    abstract public void executeDataPoint(StreamRecord<T> record,
                                          TimeWindow window);

}
