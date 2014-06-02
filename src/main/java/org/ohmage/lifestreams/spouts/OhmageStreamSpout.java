package org.ohmage.lifestreams.spouts;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.ohmage.lifestreams.models.StreamRecord;
import org.ohmage.lifestreams.models.StreamRecord.StreamRecordFactory;
import org.ohmage.lifestreams.tuples.RecordTuple;
import org.ohmage.models.OhmageStream;
import org.ohmage.models.OhmageUser;
import org.ohmage.sdk.OhmageStreamClient;
import org.ohmage.sdk.OhmageStreamIterator;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;

import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * @author changun 
 *
 * @param <T> the output data type. This class must follow
 *            the schema of the "data" field of the ohmage stream, or it can
 *            be the Jacksson "ObjectNode".
 */
public class OhmageStreamSpout<T> extends BaseLifestreamsSpout<T> {
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
	public OhmageStreamSpout(DateTime since, Class<T> c, OhmageStream stream,  String columnList) {
		super(since, 10, TimeUnit.MINUTES);
		this.c = c;
		this.stream = stream;
		this.columnList = columnList;
	}


	@Override
	protected Iterator<StreamRecord<T>> getIteratorFor(final OhmageUser user,
			DateTime since) {
		logger.trace("Fetch data for user {} from {}({}) since {}!", user.getUsername(), 
				stream.getObserverId(), 
				stream.getStreamId(), 
				since);
		try{
			final OhmageStreamIterator iter = new OhmageStreamClient(getRequester())
											.getOhmageStreamIteratorBuilder(stream, user)
											.startDate(since.minusDays(1)) // to deal with ohmage stream api bug
											.columnList(columnList)
											.build();
			
			// move the iterator pointer to the location right before the record whose timestamp >= since
			final PeekingIterator<ObjectNode> peekableIter = new PeekingIterator<ObjectNode>(iter);
			while(iter.hasNext()){
				StreamRecord<T> rec = factory.createRecord(peekableIter.peek(), user);
				if(rec.getTimestamp().compareTo(since) >= 0){
					break;
				}else{
					peekableIter.next();
				}
			}
			return new ICloseableIterator<StreamRecord<T>>() {
				@Override
				public boolean hasNext() {
					return peekableIter.hasNext();
					
				}

				@Override
				public StreamRecord<T> next() {
					ObjectNode json = peekableIter.next();
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

				@Override
				public void close() throws IOException {
						iter.close();
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
