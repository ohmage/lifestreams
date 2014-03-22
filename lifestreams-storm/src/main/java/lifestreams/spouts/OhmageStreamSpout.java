package lifestreams.spouts;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import lifestreams.models.StreamRecord;
import lifestreams.models.StreamRecord.StreamRecordFactory;

import org.joda.time.DateTime;
import org.ohmage.models.OhmageStream;
import org.ohmage.models.OhmageUser;
import org.ohmage.models.OhmageUser.OhmageAuthenticationError;
import org.ohmage.sdk.OhmageStreamClient;
import org.ohmage.sdk.OhmageStreamIterator;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
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
	DateTime startDate;

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

	private class Fetcher implements Runnable {
		OhmageUser requestee;

		Fetcher(OhmageUser requestee) {

			this.requestee = requestee;
		}

		@Override
		public void run() {

			DateTime since = pointers.get(requestee).plusMillis(1);
			OhmageStreamIterator iter;
			try {
				// if only requestee is given, use it for both requestee and requester
				iter = new OhmageStreamClient(requester == null ? requestee: requester)
						.getOhmageStreamIteratorBuilder(stream, requestee)
						.startDate(since).build();
				while (iter.hasNext()) {
					// create data point from the factory
					ObjectNode json = iter.next();
					StreamRecord<T> dp = factory.createRecord(json, requestee);
					// add the dp to the queue
					_queue.put(dp);
					// update the time
					pointers.put(requestee, dp.getTimestamp());
				}
			} catch (OhmageAuthenticationError e1) {
				e1.printStackTrace();
			} catch (IOException e1) {
				e1.printStackTrace();
			} catch (InterruptedException e) {
				return;
			} catch (Exception e) {
				e.printStackTrace();
				;
			}

		}
	}

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		_collector = collector;
		// TODO: make the number of threads adjustable
		_scheduler = Executors.newScheduledThreadPool(requestees.size());
		// schedule tasks to query the stream for each user
		for (OhmageUser requestee : requestees) {
			pointers.put(requestee, startDate);
			_scheduler.scheduleWithFixedDelay(new Fetcher(requestee), 0, 600,
					TimeUnit.SECONDS);
		}
		// create datapoint factory
		this.factory = new StreamRecordFactory<T>(dataPointClass);
	}

	@Override
	public void nextTuple() {
		try {
			while (true) {
				if (_queue.peek() != null) {
					StreamRecord<T> dp = _queue.take();
					_collector.emit(new Values(dp.getUser(), dp));
				} else {
					// sleep to let storm execute ack() and fail() methods
					Thread.sleep(100);
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
		this(stream, null, requestees, startDate, dataPointClass);
	}
	public OhmageStreamSpout(OhmageStream stream, OhmageUser requester, List<OhmageUser> requestees,
			DateTime startDate, Class<T> dataPointClass) {
		super();
		this.stream = stream;
		this.requester = requester;
		this.requestees = requestees;
		this.startDate = startDate;
		this.dataPointClass = dataPointClass;
	}
}
