package org.ohmage.lifestreams.bolts;

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

import com.esotericsoftware.kryo.Kryo;

public class UserState{
	private Task task;
	private DateTime checkpoint = null;
	private OhmageUser user;
	
	transient private LifestreamsBolt bolt;
	transient private Long curBatchId;
	transient private LinkedList<Tuple> unackedTuples;
	transient private TwoLayeredCacheMap<DateTime, List> outputCache;
	transient private Kryo kryo;
	transient private IBookkeeper bookkeeper;
	transient private Logger logger;
	transient RecordTuple curRecordTuple;
	
	public Task getTask() {
		return task;
	}
	public Long getCurBatchId() {
		return curBatchId;
	}
	public TwoLayeredCacheMap<DateTime, List> getOutputCache() {
		return outputCache;
	}

	private List<Tuple> getAllUnackedTuples(){
		return unackedTuples;
	}
	private void ackInputTuplesTil(DateTime time){

		while(!unackedTuples.isEmpty()){
			Tuple tuple = unackedTuples.getFirst();
			if(RecordTuple.getTimestampFromRawTuple(tuple).compareTo(time)<=0){
				bolt.collector.ack(unackedTuples.removeFirst());
			}else{
				break;
			}
		}
	}

	private void replay(RecordTuple tuple){
		DateTime inputTime = tuple.getTimestamp();
		// replay the corresponding output (if any)
		List<StreamRecord> outputs = outputCache.get(inputTime);
		if(outputs != null){
			logger.info(logPrefix() + "Replay {} outputs for input at {}", outputs.size(), inputTime);
			for(StreamRecord output: outputs){
				bolt.emitRecord(output, tuple.getBatchId(), Arrays.asList(tuple.getTuple()), false);
			}
		}
		if(this.checkpoint != null && inputTime.compareTo(this.checkpoint) <= 0){
			// if it is before the checkpoint, ack it to let the source know we don't need it anymore
			bolt.collector.ack(tuple.getTuple());
		}else{
			addToUnackedTuples(tuple);
		}
		return;
	}
	private void addToOutputCache(StreamRecord rec){
		DateTime key = this.curRecordTuple.getTimestamp();
		if(outputCache.containsKey(key)){
			outputCache.put(key, outputCache.get(key));
		}else{
			outputCache.put(key, new ArrayList<StreamRecord>());
		}
		outputCache.get(key).add(rec);
	}
	private void addToUnackedTuples(RecordTuple tuple){
		Iterator<Tuple> iter = this.unackedTuples.descendingIterator();
		int index = unackedTuples.size();
		while(iter.hasNext()){
			if(RecordTuple.getTimestampFromRawTuple(iter.next()).compareTo(tuple.getTimestamp())<=0){
				break;
			}
			index--;
		}
		unackedTuples.add(index, tuple.getTuple());
		
	}
	private DateTime flushOutputCacheTil(DateTime gCheckpoint){
		ArrayList<DateTime> keys = new ArrayList<DateTime>(outputCache.keySet());
		Collections.sort(keys);
		DateTime lastRemovedKey = null;
		int count = 0;
		for(DateTime key: keys){
			if(key.compareTo(gCheckpoint) <= 0){
				outputCache.remove(key);
				count ++;
				lastRemovedKey = key;
			}else{
				break;
			}
		}
		logger.info(logPrefix() + "Flush {} output cache", count);
		// TODO: Should be "Last removed output!"
		return lastRemovedKey;
	}
	public void execute(RecordTuple tuple){
		DateTime inputTime = tuple.getTimestamp();
		DateTime curTime = curRecordTuple != null ? curRecordTuple.getTimestamp() :  checkpoint;
		
		if(curTime != null && inputTime.compareTo(curTime) <= 0){
			// the state of task is ahead of input time,
			// the replay the corresponding output (if any)
			replay(tuple);
			return;
		}
		// only execute it if it is a new tuple for the task
		addToUnackedTuples(tuple);
		this.curBatchId = tuple.getBatchId();
		this.curRecordTuple = tuple;
		task.execute(tuple);
	}
	public void emit(StreamRecord output){
		// cache the output so it can be replayed later
		addToOutputCache(output);
		// emit the output and ANCHOR all the unacked tuples 
		bolt.emitRecord(output, this.curBatchId, this.getAllUnackedTuples(), true);
	}
	private String logPrefix(){
		return bolt.getComponentId()+"{" + user.getUsername() + "}:";
	}
	public void commitCheckpoint(DateTime checkpoint){
		this.checkpoint = checkpoint;
		outputCache.flushToPersistentStore();
		bookkeeper.snapshotUserState(this, kryo);
		this.ackInputTuplesTil(checkpoint);
		logger.info(logPrefix() + "Committed checkpoint {}", checkpoint);
	}
	public void executeGlobalCheckpoint(GlobalCheckpointTuple gTuple){
		DateTime lastEvictedOutput = flushOutputCacheTil(gTuple.getGlobalCheckpoint());
		if(lastEvictedOutput != null){
			bolt.emit(new GlobalCheckpointTuple(user, lastEvictedOutput),Arrays.asList(gTuple.getTuple()));
		}
		bolt.collector.ack(gTuple.getTuple());
	}
	public void streamEnd(){
		logger.info(logPrefix() + "Stream Ended, Fail all the unacked tuples and reclaim space from output cache.");
		for(Tuple tuple: this.unackedTuples){
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

	static private void fillInTransientFields(UserState state, OhmageUser user, Long curBatchId, LifestreamsBolt bolt, IBookkeeper bookkeeper, Kryo kryo){
		state.curBatchId = curBatchId;
		state.outputCache = new TwoLayeredCacheMap<DateTime, List>
							("output.cache", DateTime.class, List.class, user, bolt.getComponentId(), bookkeeper, kryo);
		state.unackedTuples =  new LinkedList<Tuple>();
		state.bookkeeper = bookkeeper;
		state.kryo = kryo;
		state.bolt = bolt;
		state.logger = LoggerFactory.getLogger(UserState.class);
		
	}
	static public UserState createOrRecoverUserState(OhmageUser user, Task templateTask, Long curBatchId, LifestreamsBolt bolt, IBookkeeper bookkeeper, Kryo kryo) {

		UserState state = null;
		UserState recoveredState = bookkeeper.recoverUserStateSnapshot(bolt.getComponentId(), user, kryo);
		// try to recover the user state snapshot from the persistent store
		if(recoveredState != null){
			fillInTransientFields(recoveredState,  user, curBatchId, bolt, bookkeeper, kryo);
			// call recover() method to recover a task from snapshot
			recoveredState.task.recover(user, recoveredState);
			state = recoveredState;
		}
		else{
			// no previous snapshot, create a new user state
			UserState newState = new UserState();
			fillInTransientFields(newState,  user, curBatchId, bolt, bookkeeper, kryo);
			newState.user = user;
			newState.task = SerializationUtils.clone(templateTask);
			newState.task.init(user, newState);
			state = newState;
		}
		

		
		return state;
	}
	public String toString() {
		return ""+bolt.getComponentId()+"."+user.getUsername();
	}
	public String getKey() {
		return toString();
	}
	static public String getKey(String cId, OhmageUser user){
		return ""+cId+"."+user.getUsername();
	}
	public OhmageUser getUser() {
		return user;
	}

}
