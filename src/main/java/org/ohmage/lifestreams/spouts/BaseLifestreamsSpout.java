package org.ohmage.lifestreams.spouts;

import backtype.storm.Config;
import backtype.storm.serialization.SerializationFactory;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import com.esotericsoftware.kryo.Kryo;
import org.joda.time.DateTime;
import org.ohmage.lifestreams.LifestreamsConfig;
import org.ohmage.lifestreams.models.StreamRecord;
import org.ohmage.lifestreams.stores.IMapStore;
import org.ohmage.lifestreams.tuples.*;
import org.ohmage.lifestreams.tuples.SpoutRecordTuple.RecordTupleMsgId;
import org.ohmage.lifestreams.tuples.StreamStatusTuple.StreamStatus;
import org.ohmage.models.OhmageUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

abstract public class BaseLifestreamsSpout<T>  extends BaseRichSpout  {

	/*** the following fields are initialized in constructor ***/
	private final TimeUnit retryDelayTimeUnit;
	private final int retryDelay;
	// from when to start the data query
	private final DateTime since;
	
	/*** the following fields are initialized in open() method ***/
	
	// the ohmage user with which we will use to query the data
	private OhmageUser requester;
    private SpoutOutputCollector collector;
	private TopologyContext context;
	private String componentId;
	Logger logger;
	private PersistentMapFactory mapFactory;
	
	/*** the following fields are initialized by default ***/
	// the queue stores the fetched data points
	private final LinkedBlockingQueue<BaseTuple> queue = new LinkedBlockingQueue<BaseTuple>();
	// thread pool
	private ScheduledExecutorService  _scheduler;
	// checkpoint of each user
	private final Map<OhmageUser, UserSpoutState> states = new HashMap<OhmageUser, UserSpoutState>();
	

	OhmageUser getRequester() {
		return requester;
	}

	public String getComponentId() {
		return componentId;
	}
	SpoutOutputCollector getCollector() {
		return collector;
	}
	DateTime getCommittedCheckpointFor(OhmageUser user){
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
		public Fetcher(OhmageUser user) {
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
			if(!iter.hasNext()){
				// no new records. return;
				return;
			}
			
			long serialId = 0;
			// emit the Head of Stream tuple
			queue.add(new StreamStatusTuple(user, batchId, StreamStatus.HEAD));
			// restart the user state
			state.newBatch(batchId);
			while(!state.isFailed() && iter.hasNext()){
				try {
					Thread.sleep(1);
				} catch (InterruptedException e) {
					return;
				}
				StreamRecord<T> record = iter.next();
				if(record.getUser() == null){
					record.setUser(user);
				}
				queue.add(new SpoutRecordTuple(record, batchId, serialId++));
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
		
		
		// init logger, context, collector fields
		this.componentId = context.getThisComponentId();
		this.logger = LoggerFactory.getLogger(componentId);
		this.context = context;
		this.collector = collector;
		
		// get serializer from topology config
		Kryo kryo = SerializationFactory.getKryo(conf);
		
		// create map factory using map store instance specified in the config
		IMapStore mapStore = (IMapStore) LifestreamsConfig.getAndDeserializeObject(conf, LifestreamsConfig.MAP_STORE_INSTANCE);
		this.mapFactory = new PersistentMapFactory((String) conf.get(Config.TOPOLOGY_NAME), mapStore, kryo);
		
		this.requester = (OhmageUser) LifestreamsConfig.getAndDeserializeObject(conf, LifestreamsConfig.LIFESTREAMS_REQUESTER);
		// ** Setup requestee list ** //
		String requesteeStr =  (String) conf.get(LifestreamsConfig.LIFESTREAMS_REQUESTEES);
		// initialize requestee array 
		String[] requesteeArray = requesteeStr.split(",");
		// ** Setup the requestee list for this spout instance** //
		
		// parameters for distributing the work among multiple spouts
		int numOfTask = context.getComponentTasks(context.getThisComponentId()).size();
		int taskIndex = context.getThisTaskIndex();

        List<OhmageUser> requestees = new ArrayList<OhmageUser>();
		
		_scheduler = Executors.newSingleThreadScheduledExecutor();
		// initialize userTimePointerMap
		for (String requesteeName : requesteeArray) {
			if(requesteeName.hashCode() % numOfTask == taskIndex){
				// use hash of requestees user name to distribute the workload to each spout
				OhmageUser requestee = new OhmageUser(requester.getServer(), requesteeName, null );
				requestees.add(requestee);
				// set start time = the next millisecond of the checkpoint or the global start time
				// defined in {DateTime since}, whichever is ahead of the other
				DateTime checkpoint = getCommittedCheckpointFor(requestee);
				DateTime start = (checkpoint != null && checkpoint.plus(1).isAfter(since)) ?
									checkpoint.plus(1) : since;
				// init the user state
				UserSpoutState state =  new UserSpoutState(requestee, this, start);
				states.put(requestee, state);
				this._scheduler.scheduleWithFixedDelay(new Fetcher(requestee), 0,
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
				 // emit a Global Checkpoint tuple
				 this.getCollector().emit(t.getValues());
				 // update the last committed serial id
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
	public BaseLifestreamsSpout(DateTime since, int retryDelay, TimeUnit unit){
		this.retryDelay = retryDelay;
		this.retryDelayTimeUnit = unit;
		this.since = since;
	}

}
