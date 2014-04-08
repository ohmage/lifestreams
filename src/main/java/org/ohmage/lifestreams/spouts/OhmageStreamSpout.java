package org.ohmage.lifestreams.spouts;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.Resource;

import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.ohmage.lifestreams.models.StreamRecord;
import org.ohmage.lifestreams.models.StreamRecord.StreamRecordFactory;
import org.ohmage.lifestreams.utils.RedisStreamStore;
import org.ohmage.models.OhmageStream;
import org.ohmage.models.OhmageUser;
import org.ohmage.models.OhmageUser.OhmageAuthenticationError;
import org.ohmage.sdk.OhmageHttpRequestException;
import org.ohmage.sdk.OhmageStreamClient;
import org.ohmage.sdk.OhmageStreamIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisException;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * @author changun 
 *
 * @param <T> the output data type. This class must follow
 *            the schema of the "data" field of the ohmage stream, or it can
 *            be the Jacksson "ObjectNode".
 */
public class OhmageStreamSpout extends BaseRichSpout {
	// stream to query
	OhmageStream stream;
	// requester should have permission to query all the requestees' data
	@Resource // use @Resource to autowire Collection
	@Qualifier("requestees")
	Set<String> requestees;
	@Autowired
	OhmageUser requester;
	
	// from when to start the data query
	DateTime since;
	
	// whether to keep the state of this spout, if false, the spout will always query the stream from "since" time
	private boolean recoverState = true;
	
	// keep the timestamp of the last point we received for each user
	private HashMap<OhmageUser, DateTime> userTimePointerMap = new HashMap<OhmageUser, DateTime>();
	// thread pool. Each requestee should have it own thread
	private ScheduledExecutorService _scheduler;
	// the queue stores the data points fetched from the ohmage
	private LinkedBlockingQueue<StreamRecord> _queue = new LinkedBlockingQueue<StreamRecord>();
	// Storm collector
	private SpoutOutputCollector _collector;
	// data point factory
	private StreamRecordFactory factory;
	// the columns to be queried
	private String columnList; 
	// the Type of the emitted records
	private Class dataPointClass;
	Logger logger;
	@Autowired
	RedisStreamStore redisStore;
	
	private class Fetcher implements Runnable {
		public void fetch(OhmageUser requestee) throws OhmageAuthenticationError, IOException, InterruptedException{
			DateTime startDate = userTimePointerMap.get(requestee).plusMillis(1);
			OhmageStreamIterator iter;
			// if only requestee is given, use it for both requestee and requester
			iter = new OhmageStreamClient(requester == null ? requestee: requester)
					.getOhmageStreamIteratorBuilder(stream, requestee)
					.startDate(startDate)
					.columnList(columnList)
					.build();
			while (iter.hasNext()) {
				// create data point from the factory
				ObjectNode json = iter.next();
				StreamRecord dp = factory.createRecord(json, requestee);
				
				// add the dp to the queue
				_queue.put(dp);
				// update the time
				userTimePointerMap.put(requestee, dp.getTimestamp());
			}
		}
		@Override
		public void run() {
			for(OhmageUser user: userTimePointerMap.keySet()){
				try{
					fetch(user);
				} catch (OhmageAuthenticationError e) {
					// wrong username/password
					throw new RuntimeException(e);
				} catch (OhmageHttpRequestException e) {
					logger.error("Timeout for {} for {}", stream, user);
					try {// slow down when timeout 
						Thread.sleep(5 * 60 * 1000);
					} catch (InterruptedException e1) {return;}
				} catch (IOException e) {
					// sth wrong with network or JSON deserialization
					throw new RuntimeException(e);
				} catch (InterruptedException e) {
					return;
				}
			}
		}
	}
	protected LinkedBlockingQueue<StreamRecord> getReturnedStreamRecordQueue(){
		return _queue;
	}
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		//** Initialize those objects that are not serializabe **/
		// initialize requestee list
		if(requestees != null && requestees.size() > 0){
			this.requestees = requestees;
		}else{
			// if the requestees are not given, use all the accessible user of requester
			this.requestees = new HashSet<String>();
			for(List<String> userList: requester.getAccessibleUsers().values()){
				for(String user: userList){
					this.requestees.add(user);
				}
			}
		}
		// initialize logger
		this.logger = LoggerFactory.getLogger(this.getClass());
		// ** End of initialize those objects that are not serializable ** //
		
		// parameters for distributing the work
		int numOfTask = context.getComponentTasks(context.getThisComponentId()).size();
		int taskIndex = context.getThisTaskIndex();
		
		_collector = collector;
		// TODO: make the number of threads adjustable
		_scheduler = Executors.newScheduledThreadPool(1);
		
		// initialize userTimePointerMap
		int i = 0;
		Jedis jedis = redisStore.getPool().getResource();
		for (String requestee : requestees) {
			if(i % numOfTask == taskIndex){
				String dateStr = jedis.hget("OhmageStream:" + stream, requestee.toString());
				DateTime startDate = (dateStr != null && recoverState &&  new DateTime(dateStr).isAfter(since)? new DateTime(dateStr):since);
				logger.info("Query {} for {} since {}", stream, requestee, startDate );
				userTimePointerMap.put(new OhmageUser(requester.getServer(), requestee, null ), startDate);
				
			}
			i++;
		}
		
		// schedule a periodic task to query the ohmage stream for each user
		_scheduler.scheduleWithFixedDelay(new Fetcher(), 0, 600, TimeUnit.SECONDS);
		redisStore.getPool().returnResource(jedis);
		// create data point factory
		this.factory =  StreamRecordFactory.createStreamRecordFactory(dataPointClass);
	}
	class MsgId{
		final OhmageStream stream;
		public MsgId(OhmageStream stream, OhmageUser user, DateTime timestamp) {
			super();
			this.stream = stream;
			this.user = user;
			this.timestamp = timestamp;
		}
		final OhmageUser user;
		final DateTime timestamp;
	}
	public void emitRecord(@SuppressWarnings("rawtypes") StreamRecord rec){
		_collector.emit(new Values(rec.getUser(), rec), new MsgId(stream, rec.getUser(), rec.getTimestamp()));
	}
	@Override
	public void nextTuple() {
		try {
			while (_queue.peek() != null) {
					StreamRecord rec = _queue.take();
					emitRecord(rec);
			} 
			// sleep for a while to save CPU if no record is available
			Thread.sleep(5000);
			
		} catch (InterruptedException e) {
			_scheduler.shutdownNow();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("user", "datapoint"));

	}
	
	@Override
	public void ack(Object id){
		 MsgId recordId = (MsgId)id;
		 Jedis jedis = redisStore.getPool().getResource();
		 // get
		 String dateStr = jedis.hget("OhmageStream:" + stream, recordId.user.toString());
		 if(dateStr == null || recordId.timestamp.isAfter(new DateTime(dateStr))){
		 	jedis.hset("OhmageStream:" + stream, recordId.user.toString(), recordId.timestamp.toString());
		 }
		 redisStore.getPool().returnResource(jedis);
	}

	/**
	 * @param stream
	 *            the ohmage stream to be queried
	 *            a list of ohmage users we will get the data from
	 * @param startDate
	 *            the start date of the query
	 * @param dataPointClass
	 *            the class of the returned data point. This class must follow
	 *            the schema of the "data" field of the ohmage stream, or it can
	 *            be the Jackason "ObjectNode".
	 */
	public OhmageStreamSpout(OhmageStream stream, DateTime startDate, Class dataPointClass, boolean recoverState, String columnList) {
		super();
		this.stream = stream;
		this.since = startDate;
		this.dataPointClass = dataPointClass;
		this.recoverState = recoverState;
		this.columnList = columnList;
		
	}
}
