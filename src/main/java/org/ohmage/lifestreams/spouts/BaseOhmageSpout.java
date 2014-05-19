package org.ohmage.lifestreams.spouts;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.Resource;

import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.ohmage.lifestreams.models.StreamRecord;
import org.ohmage.lifestreams.models.StreamRecord.StreamRecordFactory;
import org.ohmage.lifestreams.tuples.BaseTuple;
import org.ohmage.lifestreams.tuples.GlobalCheckpointTuple;
import org.ohmage.lifestreams.tuples.MsgId;
import org.ohmage.lifestreams.tuples.RecordTuple;
import org.ohmage.lifestreams.tuples.StreamStatusTuple;
import org.ohmage.lifestreams.tuples.StreamStatusTuple.StreamStatus;
import org.ohmage.models.OhmageStream;
import org.ohmage.models.OhmageUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Values;

abstract public class BaseOhmageSpout<T>  extends BaseRichSpout  {
	// requester should have permission to query all the requestees' data
	@Value("${ohmage.requestees}")
	String requesteeStr;
	@Autowired
	OhmageUser requester;
	@Autowired
	RedisBookkeeper bookkeeper;
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
	// thread  pool for committing checkpoint
	private ScheduledExecutorService _checkpointScheduler;
	// checkpoint of each user
	private Map<OhmageUser, DateTime> checkpoints = new HashMap<OhmageUser, DateTime>(); 
	private Set<OhmageUser> failFlag = Collections.newSetFromMap(new ConcurrentHashMap<OhmageUser, Boolean>());
	private TimeUnit retryDelayTimeUnit;
	private int retryDelay;
	public List<OhmageUser> getRequestees() {
		return requestees;
	}

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
		return bookkeeper.getCheckPoint(this.getComponentId(), user);
	}
	public void commitCheckpointFor(OhmageUser user, DateTime checkpoint){
		bookkeeper.setCheckPoint(this.getComponentId(), user, checkpoint);
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
				logger.info("Start getting next batch for {}", user);
				// clear failed flag
				failFlag.remove(user);
				// get the checkpoint left by the previous batch
				DateTime checkpoint = checkpoints.get(user);
				// set start time = the next millisecond of the checkpoint or the global start time
				// defined in {DateTime since}, whichever is ahead of the other
				DateTime start = checkpoint != null && checkpoint.plus(1).isAfter(since) ?
									checkpoint.plus(1) : since;
				// get a new iterator 
				Iterator<StreamRecord<T>> iter = getIteratorFor(user, start);
				queue.add(new StreamStatusTuple(user, batchId, StreamStatus.HEAD));
				
				while(!failFlag.contains(user) && iter.hasNext()){
					queue.add(new RecordTuple(iter.next(), batchId));
				}
				queue.add(new StreamStatusTuple(user, batchId, StreamStatus.END));
				
				if(failFlag.contains(user)){
					 logger.info("Failed {}", user);
					failureTimes ++;
					failFlag.remove(user);
				}
					
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
		
		logger.info("Final requestees list: {}", StringUtils.join(requesteeNames));
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
				// init the checkpoint map
				if( this.getCommittedCheckpointFor(requestee) != null){
					logger.info("Get previous checkpoint {} for {}", 
							this.getCommittedCheckpointFor(requestee), 
							requesteeName);
					this.checkpoints.put(requestee, this.getCommittedCheckpointFor(requestee));
				}
				this._scheduler.scheduleWithFixedDelay(new Fetcher(requestee, 0), 0, 
										this.retryDelay, this.retryDelayTimeUnit);
			}
			
		}
		
		_checkpointScheduler = 
				  Executors.newSingleThreadScheduledExecutor();
		_checkpointScheduler.scheduleWithFixedDelay(new CommitCheckpointWorker(), 5, 1, TimeUnit.SECONDS);

	}
	@Override
	public void ack(Object id){
		 MsgId msg = (MsgId)id;
		 OhmageUser user = msg.getUser();
		 // call different ack function for different tuple types
		 if(msg.getTupleClass().equals(RecordTuple.class)){
			 ackRecordTuple(user, (DateTime) msg.getId());
		 }else if(msg.getTupleClass().equals(GlobalCheckpointTuple.class)){
			 ackCheckpointTuple(user, (DateTime) msg.getId());
		 }
	}
	public void ackCheckpointTuple(OhmageUser user, DateTime receivedCheckpoint){
		logger.info("Receive checkpoint tuple ack {} for user {}", receivedCheckpoint, user);
	}
	class CommitCheckpointWorker implements Runnable{
		public void run(){
			for(OhmageUser user: getRequestees()){
				if(checkpoints.get(user) != null){
					DateTime commitedCheckpoint = getCommittedCheckpointFor(user);
					if( commitedCheckpoint == null || commitedCheckpoint.isBefore(checkpoints.get(user))){
						DateTime checkpoint = checkpoints.get(user);
						 GlobalCheckpointTuple t = new GlobalCheckpointTuple(user, checkpoint);
						 logger.info("Emit Checkpoint {} for {}", checkpoint, user);
						 commitCheckpointFor(user, checkpoint);
						 queue.add(t);
					}
				}
				
			}
		}
	};

	public void ackRecordTuple(OhmageUser user, DateTime timestamp){
		 if(checkpoints.get(user) == null || timestamp.isAfter(checkpoints.get(user))){
			 checkpoints.put(user, timestamp);
		 }
	}
	@Override
	public void fail(Object id){
		 MsgId msg = (MsgId)id;
		 OhmageUser user = msg.getUser();
		 if(msg.getTupleClass().equals(RecordTuple.class)){
			 DateTime receivedCheckpoint = (DateTime) msg.getId();
			 failFlag.add(user);
		 }
	}
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(RecordTuple.getFields());

	}
	public BaseOhmageSpout(int retryDelay, TimeUnit unit){
		this.retryDelay = retryDelay;
		this.retryDelayTimeUnit = unit;
	}

}
