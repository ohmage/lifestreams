package org.ohmage.lifestreams.tasks;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.joda.time.DateTime;
import org.ohmage.lifestreams.bolts.LifestreamsBolt;
import org.ohmage.lifestreams.bolts.IGenerator;
import org.ohmage.lifestreams.bolts.LifestreamsBolt;
import org.ohmage.lifestreams.bolts.UserState;
import org.ohmage.lifestreams.models.GeoLocation;
import org.ohmage.lifestreams.models.StreamRecord;
import org.ohmage.lifestreams.models.data.LifestreamsData;
import org.ohmage.lifestreams.spouts.IBookkeeper;
import org.ohmage.lifestreams.tuples.BaseTuple;
import org.ohmage.lifestreams.tuples.GlobalCheckpointTuple;
import org.ohmage.lifestreams.tuples.RecordTuple;
import org.ohmage.lifestreams.tuples.StreamStatusTuple;
import org.ohmage.lifestreams.tuples.StreamStatusTuple.StreamStatus;
import org.ohmage.lifestreams.utils.KryoSerializer;
import org.ohmage.models.OhmageUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.ImmutableList;

import backtype.storm.generated.GlobalStreamId;

/**
 * @author changun
 * 
 *         SimpleTask is the recommended way to implement a Lifestreams module.
 *         Each simple task instance performs a certain computation for one
 *         single user (i.e. it is guaranteed that the same simple task instance
 *         will only receive, and is the only one to receive, the same user's
 *         data. ). The computation is assumed to be performed on a fixed-sized
 *         time window (e.g. the "daily" geo-diameter, the weekly moving
 *         patterns etc.) The The Lifestreams framework will maintain a time
 *         window for each SimpleTask (see BasicLifestreamsBolt, and
 *         TimeWindowBolt).
 * 
 *         The init() method will be called when the task instance just created.
 *         It is the place where you can initialize the state of the
 *         computation.
 * 
 *         The executeDataPoint() will be called when receiving a new record in the
 *         current time window. It is typical to update the computation state
 *         with the new data point in this method.
 * 
 *         The finishWindow() method will be called when we have received all
 *         the records for the current time window. It is typical to finalize
 *         the computation, emit the output record, and re-initialize the
 *         computation states for the next time window in this method.
 * 
 *         In addition, the snapshotWindow() method will be called when we
 *         receive a "snapshot" command, which usually happens when the
 *         front-end applications need the partial computation results ASAP even
 *         before receiving all the data for the current time window from the
 *         user. A typical snapshotWindow() method would perform the same
 *         computation as in the finishWindow() method, but without
 *         reinitializing the computation states. emit() and emitSnapshot()
 *         method can be used to emit the output record to the next nodes in the
 *         topology or writeback to ohmage (if the target stream is specified when
 *         creating the topology).
 * @param <INPUT>
 *            the data type the task expects to receive.
 */
@SuppressWarnings("rawtypes")
public abstract class Task implements Serializable, IGenerator {

	private static final String OUTPUT_CACHE_KEY = "output.cache";
	private OhmageUser user;
	private Logger logger;
	
	private transient UserState state;
	public UserState getState(){
		return state;
	}
	// a map between the DateTime of the input tuple that triggered the output, and the output records 
	transient private TwoLayeredCacheMap outputCache;
	
	private void initUtility(OhmageUser user, UserState state){
		this.user = user;
		this.state = state;
		this.logger = LoggerFactory.getLogger(this.getClass());
	}
	public void init(OhmageUser user, UserState state) {
		initUtility(user, state);
		init();
	}
	public void recover(OhmageUser user, UserState state) {
		initUtility(user, state);
		recover();
	}
	
	protected void init(){};
	protected void recover(){};


	
	public void execute(RecordTuple input){
			executeDataPoint(input);
	}

	protected abstract void executeDataPoint(RecordTuple tuple);

	protected class RecordBuilder{
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
		public void emit(){
			if(timestamp == null || data ==null ){
				throw new RuntimeException("The required filed: data, timestamp are missing");
			}
			StreamRecord rec = new StreamRecord(user, timestamp, location, data);
			state.emit(rec);
		}
	}
	protected void checkpoint(DateTime checkpoint){
			state.commitCheckpoint(checkpoint);
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
	protected RecordBuilder createRecord(){
		return new RecordBuilder();
	}
	public Logger getLogger() {
		return logger;
	}
	public OhmageUser getUser(){
		return user;
	}
	public String getComponentId(){
		return state.getBolt().getComponentId();
	}
	@Override
	public String toString(){
		return state.getBolt().getComponentId()+this.getUser();
	}

}
