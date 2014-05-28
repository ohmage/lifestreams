package org.ohmage.lifestreams.spouts;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.ohmage.lifestreams.models.StreamRecord;
import org.ohmage.lifestreams.tuples.BaseTuple;
import org.ohmage.lifestreams.tuples.GlobalCheckpointTuple;
import org.ohmage.lifestreams.tuples.RecordTuple;
import org.ohmage.lifestreams.tuples.SpoutRecordTuple;
import org.ohmage.lifestreams.tuples.SpoutRecordTuple.RecordTupleMsgId;
import org.ohmage.lifestreams.tuples.StreamStatusTuple;
import org.ohmage.lifestreams.tuples.StreamStatusTuple.StreamStatus;
import org.ohmage.models.OhmageUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import backtype.storm.Config;
import backtype.storm.serialization.SerializationFactory;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;

import com.esotericsoftware.kryo.Kryo;

abstract public class BaseLifestreamsSpout<T>  extends BaseRichSpout  {
	// requester should have permission to query all the requestees' data
	@Value("${ohmage.requestees}")
	String requesteeStr;
	@Autowired
	OhmageUser requester;
	@Autowired
	RedisMapStore bookkeeper;
	// from when to start the data query
	@Autowired	DateTime since;
	
	private List<OhmageUser> requestees;
	private SpoutOutputCollector _collector;
	private TopologyContext context;
	private String componentId;
	protected Logger logger;
	
	// the queue stores the fetched data points
	private  LinkedBlockingQueue<BaseTuple> queue = new LinkedBlockingQueue<BaseTuple>();
	// thread pool
	private ScheduledExecutorService  _scheduler;
	// checkpoint of each user
	private Map<OhmageUser, UserSpoutState> states = new HashMap<OhmageUser, UserSpoutState>();
	private TimeUnit retryDelayTimeUnit;
	private int retryDelay;
	private PersistentMapFactory mapFactory;
	@Autowired IMapStore mapStore;

	public OhmageUser getRequester() {
		return requester;
	}

	public String getComponentId() {
		return componentId;
	}
	public SpoutOutputCollector getCollector() {
		return _collector;
	}
	public DateTime getCommittedCheckpointFor(OhmageUser user){
		return this.mapFactory.getComponentMap(this.getComponentId(), "checkpoint", String.class, DateTime.class).get(user.getUsername());
	}
	public void commitCheckpointFor(OhmageUser user, DateTime checkpoint){
		this.mapFactory.getComponentMap(this.getComponentId(), "checkpoint", String.class, DateTime.class).put(user.getUsername(), checkpoint);
	}
	public TopologyContext geTopologyContext(){
		return context;
	}
	
	protected abstract Iterator<StreamRecord<T>> getIteratorFor(OhmageUser user, DateTime since);

	public class Fetcher implements Runnable{
		final OhmageUser user;
		int failureTimes;
		public Fetcher(OhmageUser user, int failureTimes) {
			super();
			this.user = user;
			this.failureTimes = failureTimes;
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
			queue.add(new StreamStatusTuple(user, batchId, StreamStatus.HEAD));
			long serialId = 0;
			if(iter.hasNext()){
				state.newBatch(batchId);
			}
			while(!state.isFailed() && iter.hasNext()){
				try {
					Thread.sleep(1);
				} catch (InterruptedException e) {
					return;
				}
				queue.add(new SpoutRecordTuple(iter.next(), batchId, serialId++));
				state.setLastExpectedSerialId(batchId, serialId);
			}
			if(iter instanceof Closeable){
				try {
					((Closeable) iter).close();
				} catch (IOException e) {
					logger.error("Iterator close error", e);
				}
			}
			
			queue.add(new StreamStatusTuple(user, batchId, StreamStatus.END));
			state.setStreamEnded(true);

		}
		
	}
	@Override
	public void nextTuple(){
		try {
			if(!queue.isEmpty()) {
					BaseTuple tuple = queue.take();
					this.getCollector().emit(tuple.getValues(), tuple.getMessageId());
			}
			else{
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
		
		Kryo kryo = new SerializationFactory().getKryo(conf);
		this.mapFactory = new PersistentMapFactory((String) conf.get(Config.TOPOLOGY_NAME), mapStore, kryo);
		
		this.componentId = context.getThisComponentId();
		logger = LoggerFactory.getLogger(componentId);
		
		this.context = context;
		_collector = collector;
		
		// ** Setup the global requestee list ** //
		String[] requesteeNames = null;
		// initialize requestee array 
		if(requesteeStr == null || requesteeStr.length()==0){
			logger.info("Requestee list is not defined. Try to query"
					+   "all the users whose data is accessible to the requester {}", requester);
			
			// if the requestees are not given, use all the users that are accessible to the requester
			Set<String>requesteeSet = new HashSet<String>();
			for(List<String> userList: requester.getAccessibleUsers().values()){
				for(String user: userList){
					requesteeSet.add(user);
				}
			}
			requesteeNames = requesteeSet.toArray(new String[]{});

		}else{
			requesteeNames = requesteeStr.split(",");
		}
		Arrays.sort(requesteeNames);
		
		logger.info("Final requestees list: {}", StringUtils.join(requesteeNames, ','));
		// ** Setup the requestee list for this spout instance** //
		
		// parameters for distributing the work among multiple spouts
		int numOfTask = context.getComponentTasks(context.getThisComponentId()).size();
		int taskIndex = context.getThisTaskIndex();
		this.requestees = new ArrayList<OhmageUser>();
		
		_scheduler = Executors.newSingleThreadScheduledExecutor();
		// initialize userTimePointerMap
		for (String requesteeName : requesteeNames) {
			if(requesteeName.hashCode() % numOfTask == taskIndex){
				// use hash of user name to distribute the  requestees to each spout
				OhmageUser requestee = new OhmageUser(requester.getServer(), requesteeName, null );
				this.requestees.add(requestee);
				// set start time = the next millisecond of the checkpoint or the global start time
				// defined in {DateTime since}, whichever is ahead of the other
				DateTime checkpoint = getCommittedCheckpointFor(requestee);
				DateTime start = (checkpoint != null && checkpoint.plus(1).isAfter(since)) ?
									checkpoint.plus(1) : since;
				// init the user state
				UserSpoutState state =  new UserSpoutState(requestee, this, start);
				states.put(requestee, state);
				this._scheduler.scheduleWithFixedDelay(new Fetcher(requestee, 0), 0, 
										this.retryDelay, this.retryDelayTimeUnit);
			}
			
		}

	}
	@Override
	public void ack(Object id){
		if(id instanceof SpoutRecordTuple.RecordTupleMsgId ){
			RecordTupleMsgId msg = (RecordTupleMsgId) id;
			OhmageUser user = msg.getUser();
			UserSpoutState state = states.get(user);
			state.ackMsgId(msg);
			
			// how many consecutive records has been acked since last commit
			long numOfRecords = state.getAckedSerialId() - state.getLastCommittedSerialId();
			// only emit global checkpoint every 1000 records or when the stream is ended
			 if(state.isStreamEnded() || numOfRecords > 1000){
				 GlobalCheckpointTuple t = new GlobalCheckpointTuple(user, state.getCheckpoint());
				 logger.trace("Emit Global Checkpoint {} for {}", state.getCheckpoint(), user);
				 // emit a GLobalCheckpoint tuple
				 this.getCollector().emit(t.getValues());
				 // update the last commited serial id 
				 state.setLastCommittedSerial(state.getAckedSerialId());
			 }
			
			 
		}
	}

	@Override
	public void fail(Object id){
		if(id instanceof SpoutRecordTuple.RecordTupleMsgId ){
			RecordTupleMsgId msg =(SpoutRecordTuple.RecordTupleMsgId) id;
			UserSpoutState state = states.get(msg.getUser());
			state.setFailed(msg.getBatchId(), msg.getSerialId());
		}
	}
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(RecordTuple.getFields());

	}
	public BaseLifestreamsSpout(int retryDelay, TimeUnit unit){
		this.retryDelay = retryDelay;
		this.retryDelayTimeUnit = unit;
	}

}
