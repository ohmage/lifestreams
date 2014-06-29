package org.ohmage.lifestreams.spouts;

import backtype.storm.Config;
import backtype.storm.serialization.SerializationFactory;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseRichSpout;
import com.esotericsoftware.kryo.Kryo;
import org.joda.time.DateTime;
import org.ohmage.lifestreams.LifestreamsConfig;
import org.ohmage.lifestreams.models.StreamRecord;
import org.ohmage.lifestreams.stores.IMapStore;
import org.ohmage.lifestreams.stores.PersistentMapFactory;
import org.ohmage.lifestreams.tuples.BaseTuple;
import org.ohmage.lifestreams.tuples.GlobalCheckpointTuple;
import org.ohmage.lifestreams.tuples.SpoutRecordTuple;
import org.ohmage.lifestreams.tuples.StreamStatusTuple;
import org.ohmage.models.IUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * This is the skeleton class of all the Lifestreams Spouts. It create * worker threads for each to query
 * data in a fixed delay and maintains the checkpoint of each user and update the checkpoint accordingly when
 * there is a new record tuple being acked. The inheritance of this class must implement the getUsers() and
 * getIterator() methods.
 * Created by changun on 6/29/14.
 */
public abstract class BaseLifestreamsSpout<T> extends BaseRichSpout {
    /**
     * the following fields are initialized in constructor **
     */
    // retry delay
    protected final TimeUnit retryDelayTimeUnit;
    protected final int retryDelay;
    // from when to start the data query
    protected final DateTime since;

    /**
     * the following fields are initialized by default **
     */
    // the queue stores the fetched data points
    private final LinkedBlockingQueue<BaseTuple> queue = new LinkedBlockingQueue<BaseTuple>();
    // checkpoint of each user
    private final Map<IUser, UserSpoutState> states = new HashMap<IUser, UserSpoutState>();
    Logger logger;
    private SpoutOutputCollector collector;
    private String componentId;
    // a persistent userId to checkpoint map
    private Map<String, DateTime> checkpointMap;
    // thread pool
    private ScheduledExecutorService _scheduler;

    public String getComponentId() {
        return componentId;
    }

    SpoutOutputCollector getCollector() {
        return collector;
    }

    DateTime getCommittedCheckpointFor(IUser user) {
        return checkpointMap.get(user.getId());
    }

    public void commitCheckpointFor(IUser user, DateTime checkpoint) {
        checkpointMap.put(user.getId(), checkpoint);
    }


    protected abstract Iterator<StreamRecord<T>> getIteratorFor(IUser user, DateTime since);

    protected abstract List<IUser> getUsers();

    @Override
    public void nextTuple() {
        try {
            if (!queue.isEmpty()) {
                BaseTuple tuple = queue.take();
                this.getCollector().emit(tuple.getValues(), tuple.getMessageId());
            } else {
                // sleep for a while to save CPU if no record is available
                Thread.sleep(1);
            }

        } catch (InterruptedException e) {
            _scheduler.shutdownNow();
        }
    }

    @Override
    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {


        // init logger, context, collector fields
        this.componentId = context.getThisComponentId();
        this.logger = LoggerFactory.getLogger(componentId);
        this.collector = collector;

        // get serializer from topology config
        Kryo kryo = SerializationFactory.getKryo(conf);

        // create map factory using map store instance specified in the config
        IMapStore mapStore = (IMapStore) LifestreamsConfig.getAndDeserializeObject(conf, LifestreamsConfig.MAP_STORE_INSTANCE);
        PersistentMapFactory mapFactory = new PersistentMapFactory((String) conf.get(Config.TOPOLOGY_NAME), mapStore,
                kryo);
        this.checkpointMap = mapFactory.getComponentMap(this.getComponentId(), "checkpoint", String.class,
                DateTime.class);
        // parameters for distributing the work among multiple spouts
        int numOfTask = context.getComponentTasks(context.getThisComponentId()).size();
        int taskIndex = context.getThisTaskIndex();

        _scheduler = Executors.newSingleThreadScheduledExecutor();
        // initialize userTimePointerMap
        for (IUser user : getUsers()) {
            // use hash of requestees user name to distribute the workload to each spout
            if (user.hashCode() % numOfTask == taskIndex) {
                // set start time = the next millisecond of the checkpoint or the global start time
                // defined in {DateTime since}, whichever is ahead of the other
                DateTime checkpoint = getCommittedCheckpointFor(user);
                DateTime start = (checkpoint != null && checkpoint.plus(1).isAfter(since)) ?
                        checkpoint.plus(1) : since;
                // init the user state
                UserSpoutState state = new UserSpoutState(user, this, start);
                states.put(user, state);
                this._scheduler.scheduleWithFixedDelay(new Fetcher(user), 0,
                        this.retryDelay, this.retryDelayTimeUnit);
            }

        }

    }

    @Override
    public void ack(Object id) {
        if (id instanceof SpoutRecordTuple.RecordTupleMsgId) {
            SpoutRecordTuple.RecordTupleMsgId msg = (SpoutRecordTuple.RecordTupleMsgId) id;
            IUser user = msg.getUser();
            UserSpoutState state = states.get(user);
            state.ackMsgId(msg);

            // how many consecutive records has been acked since last commit
            long numOfRecords = state.getAckedSerialId() - state.getLastCommittedSerialId();
            // only emit global checkpoint every 1000 records or when the stream is ended
            if (state.isStreamEnded() || numOfRecords > 1000) {
                GlobalCheckpointTuple t = new GlobalCheckpointTuple(user, state.getCheckpoint());
                logger.trace("Emit Global Checkpoint {} for {}", state.getCheckpoint(), user);
                // emit a Global Checkpoint tuple
                this.getCollector().emit(t.getValues());
                // update the last committed serial id
                state.setLastCommittedSerial(state.getAckedSerialId());
            }


        }
    }

    @Override
    public void fail(Object id) {
        if (id instanceof SpoutRecordTuple.RecordTupleMsgId) {
            SpoutRecordTuple.RecordTupleMsgId msg = (SpoutRecordTuple.RecordTupleMsgId) id;
            UserSpoutState state = states.get(msg.getUser());
            state.setFailed(msg.getBatchId(), msg.getSerialId());
        }
    }

    public class Fetcher implements Runnable {
        final IUser user;

        public Fetcher(IUser user) {
            super();
            this.user = user;
        }

        @Override
        public void run() {

            long batchId = new DateTime().getMillis();
            // clear and update user state with new batch id
            UserSpoutState state = states.get(user);
            // get the checkpoint left by the previous batch
            DateTime checkpoint = state.getCheckpoint();
            // get a new iterator
            Iterator<StreamRecord<T>> iter = getIteratorFor(user, checkpoint);
            if (!iter.hasNext()) {
                // no new records. return;
                return;
            }

            long serialId = 0;
            // emit the Head of Stream tuple
            queue.add(new StreamStatusTuple(user, batchId, StreamStatusTuple.StreamStatus.HEAD));
            // restart the user state
            state.newBatch(batchId);
            while (!state.isFailed() && iter.hasNext()) {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    return;
                }
                StreamRecord<T> record = iter.next();
                if (record.getUser() == null) {
                    record.setUser(user);
                }
                queue.add(new SpoutRecordTuple(record, batchId, serialId++));
                state.setLastExpectedSerialId(batchId, serialId);
            }
            if (iter instanceof Closeable) {
                try {
                    ((Closeable) iter).close();
                } catch (IOException e) {
                    logger.error("Iterator close error", e);
                }
            }

            queue.add(new StreamStatusTuple(user, batchId, StreamStatusTuple.StreamStatus.END));
            state.setStreamEnded(true);

        }

    }

    protected BaseLifestreamsSpout(DateTime since, int retryDelay, TimeUnit retryDelayTimeUnit) {
        this.retryDelayTimeUnit = retryDelayTimeUnit;
        this.retryDelay = retryDelay;
        this.since = since;
    }
}
