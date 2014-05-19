package org.ohmage.lifestreams.spouts;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.Resource;

import org.joda.time.DateTime;
import org.ohmage.lifestreams.models.StreamRecord;
import org.ohmage.lifestreams.models.StreamRecord.StreamRecordFactory;
import org.ohmage.lifestreams.stores.RedisStreamStore;
import org.ohmage.lifestreams.tuples.RecordTuple;
import org.ohmage.models.OhmageStream;
import org.ohmage.models.OhmageUser;
import org.ohmage.models.OhmageUser.OhmageAuthenticationError;
import org.ohmage.sdk.OhmageHttpRequestException;
import org.ohmage.sdk.OhmageStreamClient;
import org.ohmage.sdk.OhmageStreamIterator;
import org.ohmage.sdk.OhmageStreamIterator.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.GenericTypeResolver;

import redis.clients.jedis.Jedis;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * @author changun 
 *
 * @param <T> the output data type. This class must follow
 *            the schema of the "data" field of the ohmage stream, or it can
 *            be the Jacksson "ObjectNode".
 */
public class OhmageStreamSpout<T> extends BaseOhmageSpout<T> {
	// stream to query
	OhmageStream stream;
	// data point factory
	private StreamRecordFactory factory;
	// the columns to be queried
	private String columnList; 
	// the Type of the emitted records
	private Class dataPointClass;
	private Class<T> c;
	private int rateLimit = -1;
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		
		super.open(conf, context, collector);
		// create data point factory
		this.factory =  StreamRecordFactory.createStreamRecordFactory(c);
	
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(RecordTuple.getFields());

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
	public OhmageStreamSpout(Class<T> c, OhmageStream stream,  String columnList) {
		this(c, stream,  columnList, -1);
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
	public OhmageStreamSpout(Class<T> c, OhmageStream stream,  String columnList, int rateLimit) {
		super(10, TimeUnit.MINUTES);
		this.c = c;
		this.stream = stream;
		this.columnList = columnList;
		this.rateLimit = rateLimit;
	}


	@Override
	protected Iterator<StreamRecord<T>> getIteratorFor(final OhmageUser user,
			DateTime since) {
		logger.info("Fetch data for user {} from {}({}) since {}!", user.getUsername(), 
				stream.getObserverId(), 
				stream.getStreamId(), 
				since);
		try{
			final OhmageStreamIterator iter = new OhmageStreamClient(getRequester())
											.getOhmageStreamIteratorBuilder(stream, user)
											.startDate(since)
											.columnList(columnList)
											.build();

			return new Iterator<StreamRecord<T>>(){

				@Override
				public boolean hasNext() {
					return iter.hasNext();
				}

				@Override
				public StreamRecord<T> next() {
					ObjectNode json = iter.next();
					try {
						StreamRecord rec = factory.createRecord(json, user);
						return rec;
					} catch (Exception e){
						logger.error("convert ohmage record error", e);
						throw new RuntimeException(e);
					}
				}

				@Override
				public void remove() {
					throw new UnsupportedOperationException();
					
				}
				
			};

		}catch(Exception e){
		logger.error("Fetch data error for user {} from {}({}) since {}!", user.getUsername(), 
						stream.getObserverId(), 
						stream.getStreamId(), 
						since);
		logger.error("Trace: ", e);
		}
		return null;
	}
}
