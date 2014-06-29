package org.ohmage.lifestreams.bolts;

import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImpl;
import org.apache.commons.lang3.SerializationUtils;
import org.joda.time.DateTime;
import org.ohmage.lifestreams.models.StreamRecord;
import org.ohmage.lifestreams.stores.PersistentMapFactory;
import org.ohmage.lifestreams.tasks.Task;
import org.ohmage.lifestreams.tasks.TwoLayeredCacheMap;
import org.ohmage.lifestreams.tuples.GlobalCheckpointTuple;
import org.ohmage.lifestreams.tuples.RecordTuple;
import org.ohmage.models.IUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.*;

/**
 * UserTaskState stores and maintains the computation state to enforce the
 * just-once and in-order data processing and allow the computation state to be
 * recovered after crash/failure. A UserTaskState is wrapping around a Task: all
 * input/output to and from a task must go through its corresponding UserTaskState. A
 * task can commit a checkpoint through UserTaskState to make sure that the
 * computation state can be recovered and resumed from the checkpoint, after crash or failure.
 * <p/>
 * <ul>
 * <li>UserTaskState achieves the "just once" processing by maintaining a output
 * cache. When the task emits a new output tuple, the UserTaskState will cache the
 * tuple in a output buffer. When
 * receiving an input that is "before" the last seen input or the last
 * checkpoint, instead of executing it again (which violates the "just-once"
 * guarantee), the UserTaskState will look up the output cache and "replay" the
 * corresponding output. (See {@link #execute(RecordTuple)} and
 * {@link #replay(RecordTuple)} )</li>
 * <p/>
 * <li>UserTaskState provide Tasks with the checkpoint mechanism. When a task
 * commits a checkpoint, the UserTaskState makes a snapshot of itself, including
 * the state of the task, along with the the output cache, and store them in a persistent
 * storage so that they can be restored later. In additional to that,
 * UserTaskState will ack all the tuples received before the checkpoint to let
 * the topology knows that this task does not need to receive these tuples anymore
 * (See {@link #commitCheckpoint(DateTime)} ).</li>
 * <p/>
 * <li>
 * To prevent the output cache to grow infinitely, we depends on the
 * GlobalCheckpoint mechanism provided by {@link org.ohmage.lifestreams.spouts.Ohmage20Spout}. When a
 * input tuple and its dependency tree (i.e. the tuples that were derived from
 * it) have been fully acked, the spout will emit a GlobalCheckpoint message to
 * its immediate tasks to let them know that they can remove their output cached
 * in the output cache that are derived from this tuple; these tasks then will
 * do same to their child tasks too so on so forth (See
 * {@link #executeGlobalCheckpoint(GlobalCheckpointTuple)} ). This operation is
 * equivalent to reclaiming the space taken up by the dependency tree.</li>
 * <p/>
 * </ul>
 *
 * @author changun
 */
public class UserTaskState {
    private Task task;
    private DateTime checkpoint = null;
    private IUser user;

    transient private LifestreamsBolt bolt;
    transient private LinkedList<Tuple> unackedTuples;
    transient private LinkedList<DateTime> unackedTupleTimes;
    transient private TwoLayeredCacheMap<DateTime, ArrayList> outputCache;
    transient private Logger logger;
    private transient RecordTuple curRecordTuple;


    public Task getTask() {
        return task;
    }


    private void ackInputTuplesTil(DateTime time) {

        while (!unackedTuples.isEmpty()) {
            DateTime tupleTime = unackedTupleTimes.getFirst();
            if (tupleTime.compareTo(time) <= 0) {
                bolt.collector.ack(unackedTuples.removeFirst());
                unackedTupleTimes.removeFirst();
            } else {
                break;
            }
        }
    }

    private void replay(RecordTuple tuple) {
        DateTime inputTime = tuple.getTimestamp();
        // replay the corresponding output (if any)
        @SuppressWarnings({"unchecked", "rawtypes"})
        List<StreamRecord> outputs = outputCache.get(inputTime);
        if (outputs != null) {
            logger.trace(logPrefix() + "Replay {} outputs for input at {}",
                    outputs.size(), inputTime);
            for (StreamRecord output : outputs) {
                bolt.emitRecord(output, Arrays.asList(tuple.getTuple()), false);
            }
        }
        if (this.checkpoint != null
                && inputTime.compareTo(this.checkpoint) <= 0) {
            // if it is before the checkpoint, ack it to let the source know we
            // don't need it anymore
            bolt.collector.ack(tuple.getTuple());
        } else {
            addToUnackedTuples(tuple);
        }
    }

    private void addToOutputCache(StreamRecord rec) {
        DateTime key = this.curRecordTuple.getTimestamp();
        if (outputCache.containsKey(key)) {
            outputCache.put(key, outputCache.get(key));
        } else {
            outputCache.put(key, new ArrayList<StreamRecord>());
        }
        outputCache.get(key).add(rec);
    }

    private void addToUnackedTuples(RecordTuple tuple) {
        Iterator<DateTime> iter = this.unackedTupleTimes.descendingIterator();
        int index = unackedTuples.size();
        while (iter.hasNext()) {
            if (iter.next().compareTo(tuple.getTimestamp()) <= 0) {
                break;
            }
            index--;
        }

        Field field;
        try {
            // null the "values" field of the tuple object to
            // save memory space
            field = TupleImpl.class.getDeclaredField("values");
            field.setAccessible(true);
            field.set(tuple.getTuple(), null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        unackedTupleTimes.add(index, tuple.getTimestamp());
        unackedTuples.add(index, tuple.getTuple());

    }

    private DateTime flushOutputCacheTil(DateTime gCheckpoint) {
        ArrayList<DateTime> keys = new ArrayList<DateTime>(outputCache.keySet());
        Collections.sort(keys);
        DateTime lastRemovedOutputTime = null;
        int count = 0;
        for (DateTime key : keys) {
            if (key.compareTo(gCheckpoint) <= 0) {
                ArrayList<StreamRecord> removedOutputArray = outputCache.remove(key);
                count++;
                lastRemovedOutputTime = removedOutputArray.get(removedOutputArray.size() - 1).getTimestamp();
            } else {
                break;
            }
        }
        logger.trace(logPrefix() + "Flush {} output cache", count);
        return lastRemovedOutputTime;
    }

    public void execute(RecordTuple tuple) {
        DateTime inputTime = tuple.getTimestamp();
        DateTime curTime = curRecordTuple != null ? curRecordTuple
                .getTimestamp() : checkpoint;

        if (curTime != null && inputTime.compareTo(curTime) <= 0) {
            // the state of task is ahead of input time,
            // the replay the corresponding output (if any)
            replay(tuple);
            return;
        }
        // only execute it if it is a new tuple for the task
        addToUnackedTuples(tuple);
        this.curRecordTuple = tuple;
        task.execute(tuple);
    }

    /**
     * Emit a output to the child Tasks.
     *
     * @param output record to emit
     */
    public void emit(StreamRecord output) {
        // cache the output so that it can be replayed after crash
        addToOutputCache(output);
        // emit the output and ANCHOR all the current tuples
        bolt.emitRecord(output, Arrays.asList(this.curRecordTuple.getTuple()), true);
    }

    private String logPrefix() {
        return bolt.getComponentId() + "{" + user.getId() + "}:";
    }

    private void makeSnapshot() {
        bolt.getPersistentStateMap().put(user.getId(), this);
    }

    /**
     * Make a checkpoint. A task commits a checkpoint to ensure that the
     * computation will be resumed from current state. When specifing the
     * checkpoint time, please be aware that a task WILL NOT receive any tuple
     * whose timestamp is before the checkpoint, even if these tuple have not
     * been processed before.
     *
     * @param checkpoint The checkpoint time
     */
    public void commitCheckpoint(DateTime checkpoint) {
        this.checkpoint = checkpoint;
        outputCache.persist();
        makeSnapshot();
        this.ackInputTuplesTil(checkpoint);
    }

    /**
     * Make a checkpoint with the time of the currently executed tuple.
     */
    public void commitCheckpoint() {
        commitCheckpoint(this.curRecordTuple.getTimestamp());
    }

    /**
     * When receiving a global checkpoint. Flush the cached output whose source
     * input tuple is "before" the global checkpoint (I.e. its timestamp is
     * earlier than the global checkpoint).
     *
     * @param gTuple global check point tuple
     */
    public void executeGlobalCheckpoint(GlobalCheckpointTuple gTuple) {
        DateTime lastEvictedOutput = flushOutputCacheTil(gTuple.getGlobalCheckpoint());
        if (lastEvictedOutput != null) {
            bolt.emit(new GlobalCheckpointTuple(user, lastEvictedOutput),
                    Arrays.asList(gTuple.getTuple()));
        }
        bolt.collector.ack(gTuple.getTuple());
    }

    /**
     * No more data will be received in at least a short time. Reclaim all the
     * temporary data, and let the computation to be resumed from the last
     * checkpoint.
     */
    public void streamEnd() {
        logger.trace(logPrefix()
                + "Stream End. Checkpoint: {}. Fail all the unacked tuples.", this.checkpoint);
        for (Tuple tuple : this.unackedTuples) {
            bolt.collector.fail(tuple);
        }
        // reclaim memory space
        curRecordTuple = null;
        unackedTuples.clear();
        outputCache.clearLocalCache();
        task = null;
        checkpoint = null;
    }

    public LifestreamsBolt getBolt() {
        return bolt;
    }

    public boolean isEnded() {
        return task == null;
    }

    static private void fillInTransientFields(UserTaskState state,
                                              PersistentMapFactory mapFactory, LifestreamsBolt bolt) {
        Map<DateTime, ArrayList> pMap = mapFactory.getUserTaskMap(bolt.getComponentId(),
                state.getUser(),
                "output_cache",
                DateTime.class, ArrayList.class);

        state.outputCache = new TwoLayeredCacheMap<DateTime, ArrayList>(pMap);
        state.unackedTuples = new LinkedList<Tuple>();
        state.unackedTupleTimes = new LinkedList<DateTime>();
        state.bolt = bolt;
        state.logger = LoggerFactory.getLogger(UserTaskState.class);
    }


    public String toString() {
        return "" + bolt.getComponentId() + "." + user.getId();
    }

    IUser getUser() {
        return user;
    }

    /**
     * Create or recover the state (if exists) from the persistent storage.
     *
     * @param user         The user.
     * @param templateTask The task to perform.
     * @param bolt         The bolt containing this state.
     * @param mapFactory   Factory for persistent map store
     * @return A user state instance that is either newly created or recovered from the persistent map store.
     */
    public static UserTaskState createOrRecoverUserState(
            LifestreamsBolt bolt, IUser user, Task templateTask,
            PersistentMapFactory mapFactory) {
        UserTaskState state;
        UserTaskState recoveredState = bolt.getPersistentStateMap().get(user.getId());
        // try to recover the user state snapshot from the persistent store
        if (recoveredState != null) {
            fillInTransientFields(recoveredState, mapFactory, bolt);
            // run recover() method to recover a task from snapshot
            recoveredState.task.recover(user, recoveredState, mapFactory);
            state = recoveredState;
        } else {
            // no previous snapshot, create a new user state
            UserTaskState newState = new UserTaskState();
            newState.user = user;
            fillInTransientFields(newState, mapFactory, bolt);
            // make a copy of the template Task
            newState.task = SerializationUtils.clone(templateTask);
            // run init()
            newState.task.init(user, newState, mapFactory);
            state = newState;
        }
        return state;
    }

}
