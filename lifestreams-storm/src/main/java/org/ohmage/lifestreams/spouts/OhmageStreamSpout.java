package org.ohmage.lifestreams.spouts;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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
public class OhmageStreamSpout<T extends Object> extends BaseRichSpout {
	// stream to query
	OhmageStream stream;
	// requester should have permission to query all the requestees' data
	List<OhmageUser> requestees;
	OhmageUser requester;
	
	// from when to start the data query
	DateTime since;
	
	// whether to keep the state of this spout, if false, the spout will always query the stream from "since" time
	private boolean recoverState;
	
	// the size of timeframe to be queried in each request (this is a work around for 2.16's performance issue)
	Duration timeframeSizeOfEachRequest = null;
	// keep the timestamp of the last point we received for each user
	private HashMap<OhmageUser, DateTime> pointers = new HashMap<OhmageUser, DateTime>();
	// thread pool. Each requestee should have it own thread
	private ScheduledExecutorService _scheduler;
	// the queue stores the data points fetched from the ohmage
	private LinkedBlockingQueue<StreamRecord<T>> _queue = new LinkedBlockingQueue<StreamRecord<T>>();
	// Storm collector
	private SpoutOutputCollector _collector;
	// data point factory
	private StreamRecordFactory<T> factory;

	private Class<T> dataPointClass;
	Logger logger;

	private class Fetcher implements Runnable {
		OhmageUser requestee;

		Fetcher(OhmageUser requestee) {

			this.requestee = requestee;
		}
		@Override
		public void run() {

			DateTime startDate = pointers.get(requestee).plusMillis(1);
			OhmageStreamIterator iter;
			try {
				do{
					// run this loop once if timeframeSizeOfEachRequest  is not specified, otherwise, run until startDate > now
					DateTime endDate = timeframeSizeOfEachRequest != null ? startDate.plus(timeframeSizeOfEachRequest) :DateTime.now();
					// if only requestee is given, use it for both requestee and requester
					iter = new OhmageStreamClient(requester == null ? requestee: requester)
							.getOhmageStreamIteratorBuilder(stream, requestee)
							.startDate(startDate).endDate(endDate).build();
					while (iter.hasNext()) {
						// create data point from the factory
						ObjectNode json = iter.next();
						StreamRecord<T> dp = factory.createRecord(json, requestee);
						// add the dp to the queue
						_queue.put(dp);
						// update the time
						pointers.put(requestee, dp.getTimestamp());
					}
					startDate = endDate;
					
				}while(timeframeSizeOfEachRequest != null && startDate.isBefore(DateTime.now()));
			} catch (OhmageAuthenticationError e) {
				throw new RuntimeException(e);
			} catch (InterruptedException e) {
				return;
			} catch (OhmageHttpRequestException e) {
				logger.error("Timeout for {} for {}", stream, requestee);
				try {
					// slow down when timeout 
					Thread.sleep(5 * 60 * 1000);
				} catch (InterruptedException e1) {
				}
			} catch (IOException e) {
				throw new RuntimeException(e);
			}

		}
	}

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		
		this.logger = LoggerFactory.getLogger(this.getClass());
		// parameters for distributing the work
		int numOfTask = context.getComponentTasks(context.getThisComponentId()).size();
		int taskIndex = context.getThisTaskIndex();
		
		_collector = collector;
		// TODO: make the number of threads adjustable
		_scheduler = Executors.newScheduledThreadPool(1);
		// schedule tasks to query the stream for each user
		int i = 0;
		Jedis jedis = RedisStreamStore.getPool().getResource();
		for (OhmageUser requestee : requestees) {
			if(i % numOfTask == taskIndex){
				
				String dateStr = jedis.hget("OhmageStream:" + stream, requestee.toString());
				DateTime startDate = (dateStr != null && recoverState ? new DateTime(dateStr):since);
				logger.info("Query {} for {} since {}", stream, requestee, startDate );
				pointers.put(requestee, startDate);
				_scheduler.scheduleWithFixedDelay(new Fetcher(requestee), 0, 600, TimeUnit.SECONDS);
			}
			i++;
		}
		 RedisStreamStore.getPool().returnResource(jedis);
		// create datapoint factory
		this.factory = new StreamRecordFactory<T>(dataPointClass);
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
	@Override
	public void nextTuple() {
		try {
			while (true) {
				if (_queue.peek() != null) {
					StreamRecord<T> dp = _queue.take();
					_collector.emit(new Values(dp.getUser(), dp), new MsgId(stream, dp.getUser(), dp.getTimestamp()));
				} else {
					return;
				}
			}
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
		 Jedis jedis = RedisStreamStore.getPool().getResource();
		 // get
		 String dateStr = jedis.hget("OhmageStream:" + stream, recordId.user.toString());
		 if(dateStr == null || recordId.timestamp.isAfter(new DateTime(dateStr))){
		 	jedis.hset("OhmageStream:" + stream, recordId.user.toString(), recordId.timestamp.toString());
		 }
		 RedisStreamStore.getPool().returnResource(jedis);
	}

	/**
	 * @param stream
	 *            the ohmage stream to be queried
	 * @param requestees
	 *            a list of ohmage users we will get the data from
	 * @param startDate
	 *            the start date of the query
	 * @param dataPointClass
	 *            the class of the returned data point. This class must follow
	 *            the schema of the "data" field of the ohmage stream, or it can
	 *            be the Jackason "ObjectNode".
	 */
	public OhmageStreamSpout(OhmageStream stream, List<OhmageUser> requestees,
			DateTime startDate, Class<T> dataPointClass) {
		this(stream, null, requestees, startDate, dataPointClass, null, true);
	}
	public OhmageStreamSpout(OhmageStream stream, OhmageUser requester, List<OhmageUser> requestees,
			DateTime startDate, Class<T> dataPointClass) {
		this(stream, requester, requestees, startDate, dataPointClass, null, true);
	}
	public OhmageStreamSpout(OhmageStream stream, OhmageUser requester, List<OhmageUser> requestees,
			DateTime startDate, Class<T> dataPointClass, Duration timeframeSizeOfEachRequest, boolean recoverState) {
		super();
		this.stream = stream;
		this.requester = requester;
		this.requestees = requestees;
		this.since = startDate;
		this.dataPointClass = dataPointClass;
		this.timeframeSizeOfEachRequest = timeframeSizeOfEachRequest;
		this.recoverState = recoverState;
	}
}
