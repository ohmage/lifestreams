package org.ohmage.lifestreams.tasks;

import org.joda.time.DateTime;
import org.ohmage.lifestreams.bolts.IGenerator;
import org.ohmage.lifestreams.bolts.UserTaskState;
import org.ohmage.lifestreams.models.GeoLocation;
import org.ohmage.lifestreams.models.StreamRecord;
import org.ohmage.lifestreams.stores.PersistentMapFactory;
import org.ohmage.lifestreams.tuples.RecordTuple;
import org.ohmage.models.IUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Set;

/**
 * Tasks are the main bulding blocks of a Lifestreams topology and is where the
 * data processing/manipulation logic should be specified. When there are more
 * than one user, multiple task instances will be created through the deep copy
 * mechanism. Each task instance is only responsible for processing the data
 * from a single user and will only receive the data from that user too.
 * Lifestreams gurantees that a task will receive and process the data
 * "in-order and just-once" without missing/skipping any data points.
 * <p/>
 * A typical Task should implement the following methods: The {@link #init()} or
 * {@link #recover()} methods is called when a task is newly created or
 * recovered from the previous state. {@link #executeDataPoint(RecordTuple)} is
 * called when receiving a new data point.
 * <p/>
 * Please use the {@link #createRecord()} method to create a output record that
 * will be emitted to the child task.
 * <p/>
 * In addition, a task can make a checkpoint by calling
 * {@link #checkpoint(DateTime)} so that when crash/failure happens, the
 * computation can and will be restored to the state when the checkpoint was
 * made.
 * <p/>
 * The timing of making a checkpoin is critical to the system performance.
 * Making checkpoint too often makes the system slow as it incurs much overhead
 * to make a checkpoint (including: serializing/saving the computation state).
 * But, making checkpoint too rarely is also harmful as the system cannot
 * reclaim the space taken up by the cached output tuples if no checkpoint is
 * made. (The output cache is required to maintain the "just-once" gurantee. See
 * {@link UserTaskState}.)
 * <p/>
 * Potential timing to make checkpoint are: 1) after performing operations with
 * external entities (e.g. after uploading the data to a remote server, or after
 * pushing notification to a user). Making a checkpoint ensures that these
 * operation won't be repeated. 2) after finish processing a certain number of
 * tuples or after finishing all the tuple belonging to a certain timeframe.
 */
@SuppressWarnings("rawtypes")
public abstract class Task implements Serializable, IGenerator {

    private transient IUser user;
    private transient Logger logger;

    private transient UserTaskState state;
    private transient PersistentMapFactory mapFactory;

    protected PersistentMapFactory getMapFactory() {
        return mapFactory;
    }

    UserTaskState getState() {
        return state;
    }

    private void initUtility(IUser user, UserTaskState state, PersistentMapFactory factory) {
        this.user = user;
        this.state = state;
        this.mapFactory = factory;
        this.logger = LoggerFactory.getLogger(this.getClass());
    }

    public void init(IUser user, UserTaskState state, PersistentMapFactory factory) {
        initUtility(user, state, factory);
        init();
    }

    public void recover(IUser user, UserTaskState state, PersistentMapFactory factory) {
        initUtility(user, state, factory);
        recover();
    }

    void init() {
    }

    protected void recover() {
    }

    public void execute(RecordTuple input) {
        executeDataPoint(input);
    }

    protected abstract void executeDataPoint(RecordTuple tuple);

    protected class RecordBuilder {
        GeoLocation location;
        DateTime timestamp;
        Object data;

        public GeoLocation getLocation() {
            return location;
        }

        public RecordBuilder setLocation(GeoLocation location) {
            this.location = location;
            return this;
        }

        public DateTime getTimestamp() {
            return timestamp;
        }

        public RecordBuilder setTimestamp(DateTime timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public Object getData() {
            return data;
        }

        public RecordBuilder setData(Object data) {
            this.data = data;
            return this;
        }

        public void emit() {
            if (timestamp == null || data == null) {
                throw new RuntimeException(
                        "The required filed: data, timestamp are missing");
            }
            StreamRecord rec = new StreamRecord(user, timestamp, location, data);
            state.emit(rec);
        }
    }

    protected void checkpoint(DateTime checkpoint) {
        state.commitCheckpoint(checkpoint);
    }

    protected void checkpoint() {
        state.commitCheckpoint();
    }

    @Override
    public String getGeneratorId() {
        return this.getClass().getName();
    }

    @Override
    public String getTopologyId() {
        return state.getBolt().getTopologyId();
    }

    @Override
    public Set<String> getSourceIds() {
        return state.getBolt().getSourceIds();
    }

    protected RecordBuilder createRecord() {
        return new RecordBuilder();
    }

    protected Logger getLogger() {
        return logger;
    }

    public IUser getUser() {
        return user;
    }

    public String getComponentId() {
        return state.getBolt().getComponentId();
    }

    @Override
    public String toString() {
        return state.getBolt().getComponentId() + this.getUser();
    }

}
