package org.ohmage.lifestreams.bolts;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.lang3.SerializationUtils;
import org.joda.time.DateTime;
import org.ohmage.lifestreams.models.StreamRecord;
import org.ohmage.lifestreams.spouts.IBookkeeper;
import org.ohmage.lifestreams.tasks.Task;
import org.ohmage.lifestreams.tasks.TwoLayeredCacheMap;
import org.ohmage.lifestreams.tuples.GlobalCheckpointTuple;
import org.ohmage.lifestreams.tuples.RecordTuple;
import org.ohmage.models.OhmageUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImpl;

import com.esotericsoftware.kryo.Kryo;

/**
 * UserTaskState stores and maintains the computation state to enforce the
 * just-once and in-order data processing and allow the computation state to be
 * recovered after crash/failure. A UserTaskState is warpping around a Task: all
 * the input/output to and from that task must go through the UserTaskState. A
 * task can commit a checkpoint through UserTaskState to make sure that the
 * computation will be resumed from the checkpoint, after crash or termination.
 * 
 * <ul>
 * <li>UserTaskState enforces the "just once" processing by maintaining a output
 * cache. When the task emit a new output tuple, in addition to emitting the
 * tuple to the next tasks, the tuple will be cached in a output buffer. When
 * receiving a input that is "before" the last seen input or the last
 * checkpoint, instead of executing it again (which violates the "just-once"
 * guranttee), the UserTaskState will look up the output cache and "replay" the
 * corresponding output. (See {@link #execute(RecordTuple)} and
 * {@link #replay(RecordTuple)} )</li>
 * 
 * <li>UserTaskState provide Tasks with the checkpoint mechanism. When a task
 * commit a checkpoint, the UserTaskState make a snapshot of itself, including
 * the state of the task and output cache, and store them in a persistent
 * storage so that it can be restored later. In additional to that,
 * UserTaskState will ack all the tuples received before the checkpoint to let
 * the topology knows that this task has finish process these tuples (so that
 * they won't be replayed) (See {@link #commitCheckpoint(DateTime)} ).</li>
 * 
 * <li>
 * To prevent the output cache to grow infinitely, we depends on the
 * GlobalCheckpoint mechanism provided by {@link BaseLifestreamsSpout}. When a
 * input tuple and its dependency tree (i.e. the tuples that were derived from
 * it) have been fully acked, the spout will emit a GlobalCheckpoint message to
 * its immediate tasks to let them know that they can remove their output cached
 * in the output cache that are derived from this tuple; these tasks then will
 * do same to their child tasks too so on so forth (See
 * {@link #executeGlobalCheckpoint(GlobalCheckpointTuple)} ). This operation is
 * equivalent to reclaimming the space taken up by the denpendency tree.</li>
 * 
 * </ul>
 * 
 * @author changun
 * 
 */
public class UserTaskState {
	private Task task;
	private DateTime checkpoint = null;
	private OhmageUser user;

	transient private LifestreamsBolt bolt;
	transient private LinkedList<Tuple> unackedTuples;
	transient private LinkedList<DateTime> unackedTupleTimes;
	transient private TwoLayeredCacheMap<DateTime, List> outputCache;
	transient private Kryo kryo;
	transient private IBookkeeper bookkeeper;
	transient private Logger logger;
	transient RecordTuple curRecordTuple;

	public Task getTask() {
		return task;
	}

	public TwoLayeredCacheMap<DateTime, List> getOutputCache() {
		return outputCache;
	}

	private List<Tuple> getAllUnackedTuples() {
		return unackedTuples;
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
		@SuppressWarnings({ "unchecked", "rawtypes" })
		List<StreamRecord> outputs = outputCache.get(inputTime);
		if (outputs != null) {
			logger.info(logPrefix() + "Replay {} outputs for input at {}",
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
		return;
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
			// null the values field of the tuple object using reflection to
			// save space
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
		DateTime lastRemovedKey = null;
		int count = 0;
		for (DateTime key : keys) {
			if (key.compareTo(gCheckpoint) <= 0) {
				outputCache.remove(key);
				count++;
				lastRemovedKey = key;
			} else {
				break;
			}
		}
		logger.info(logPrefix() + "Flush {} output cache", count);
		// TODO: Should be "Last removed output!"
		return lastRemovedKey;
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
	 * @param output
	 */
	public void emit(StreamRecord output) {
		// cache the output so that it can be replayed after crash
		addToOutputCache(output);
		// emit the output and ANCHOR all the current tuples
		bolt.emitRecord(output, Arrays.asList(this.curRecordTuple.getTuple()),
				true);
	}

	private String logPrefix() {
		return bolt.getComponentId() + "{" + user.getUsername() + "}:";
	}

	/**
	 * Make a checkpoint. A task commits a checkpoint to ensure that the
	 * computation will be resumed from current state. When specifing the
	 * checkpoint time, please be aware that a task WILL NOT receive any tuple
	 * whose timestamp is before the checkpoint, even if these tuple have not
	 * been processed before.
	 * 
	 * @param checkpoint
	 *            The checkpoint time
	 */
	public void commitCheckpoint(DateTime checkpoint) {
		this.checkpoint = checkpoint;
		outputCache.flushToPersistentStore();
		bookkeeper.snapshotUserState(this, kryo);
		this.ackInputTuplesTil(checkpoint);
	}

	/**
	 * When receiving a global checkpoint. Flush the cached output whose source
	 * input tuple is "before" the global checkpoint (I.e. its timestamp is
	 * ealier than the global checkpoint).
	 * 
	 * @param gTuple
	 */
	public void executeGlobalCheckpoint(GlobalCheckpointTuple gTuple) {
		DateTime lastEvictedOutput = flushOutputCacheTil(gTuple
				.getGlobalCheckpoint());
		if (lastEvictedOutput != null) {
			bolt.emit(new GlobalCheckpointTuple(user, lastEvictedOutput),
					Arrays.asList(gTuple.getTuple()));
		}
		bolt.collector.ack(gTuple.getTuple());
	}

	/**
	 * No more data will be recived in at least a short time. Reclaim all the
	 * temporary data, and let the computation to be resumed from the last
	 * checkpoint.
	 */
	public void streamEnd() {
		logger.info(logPrefix()
				+ "Stream Ended, Fail all the unacked tuples and reclaim space from output cache.");
		for (Tuple tuple : this.unackedTuples) {
			bolt.collector.fail(tuple);
		}
		curRecordTuple = null;
		unackedTuples.clear();
		outputCache.clearLocalCache();
		task = null;
		checkpoint = null;
	}

	public LifestreamsBolt getBolt() {
		return bolt;
	}

	static private void fillInTransientFields(UserTaskState state,
			OhmageUser user, LifestreamsBolt bolt, IBookkeeper bookkeeper,
			Kryo kryo) {
		state.outputCache = new TwoLayeredCacheMap<DateTime, List>(
				"output.cache", DateTime.class, List.class, user,
				bolt.getComponentId(), bookkeeper, kryo);
		state.unackedTuples = new LinkedList<Tuple>();
		state.unackedTupleTimes = new LinkedList<DateTime>();
		state.bookkeeper = bookkeeper;
		state.kryo = kryo;
		state.bolt = bolt;
		state.logger = LoggerFactory.getLogger(UserTaskState.class);

	}

	/**
	 * Create or recover the state (if exists) from the persistent storage.
	 * 
	 * @param user
	 *            The user.
	 * @param templateTask
	 *            The task to perform.
	 * @param bolt
	 *            The bolt containing this state.
	 * @param bookkeeper
	 *            Helper functions to access persistent storage.
	 * @param kryo
	 *            Kryo Java object serializer.
	 * @return
	 */
	static public UserTaskState createOrRecoverUserState(OhmageUser user,
			Task templateTask, LifestreamsBolt bolt, IBookkeeper bookkeeper,
			Kryo kryo) {

		UserTaskState state = null;
		UserTaskState recoveredState = bookkeeper.recoverUserStateSnapshot(
				bolt.getComponentId(), user, kryo);
		// try to recover the user state snapshot from the persistent store
		if (recoveredState != null) {
			fillInTransientFields(recoveredState, user, bolt, bookkeeper, kryo);
			// call recover() method to recover a task from snapshot
			recoveredState.task.recover(user, recoveredState);
			state = recoveredState;
		} else {
			// no previous snapshot, create a new user state
			UserTaskState newState = new UserTaskState();
			fillInTransientFields(newState, user, bolt, bookkeeper, kryo);
			newState.user = user;
			newState.task = SerializationUtils.clone(templateTask);
			newState.task.init(user, newState);
			state = newState;
		}
		return state;
	}

	public String toString() {
		return "" + bolt.getComponentId() + "." + user.getUsername();
	}

	public String getKey() {
		return toString();
	}

	static public String getKey(String cId, OhmageUser user) {
		return "" + cId + "." + user.getUsername();
	}

	public OhmageUser getUser() {
		return user;
	}

}
