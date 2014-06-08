package org.ohmage.lifestreams.stores;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.joda.time.Interval;
import org.ohmage.lifestreams.models.StreamRecord;
import org.ohmage.lifestreams.models.StreamRecord.StreamRecordFactory;
import org.ohmage.models.OhmageStream;
import org.ohmage.models.OhmageUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.ArrayList;
import java.util.List;


public class RedisStreamStore implements IStreamStore {

	private static final Logger logger = LoggerFactory.getLogger(RedisStreamStore.class);
	private String host = "localhost";

	transient private JedisPool pool;
	JedisPool getPool(){
		if(pool == null){
			pool = new JedisPool(new JedisPoolConfig(), host);
		}
		return pool;
	}
	// store a record to the Redis server. The records of the same pair of stream + user will be 
	// stored in a hash table, with the keys as the string representation of the data (i.e. output of data.getString().)
	// In most cases, where the  data object is a subclass of the LifestreamsData, see lifestreams.models.data.LifestreamsData
	// for the format of this representation.
	@Override
	public void upload(OhmageStream stream, StreamRecord rec) {
		Jedis jedis = getPool().getResource();
		jedis.select(0);
		String key = rec.getData().toString() + rec.getTimestamp();
		String value;
		try {
			ObjectMapper mapper = new ObjectMapper();
			value = mapper.writeValueAsString(rec.toObserverDataPoint());
			jedis.hset(rec.getUser().toString() + stream.toString(), key, value);
		} catch (JsonProcessingException e) {
			throw new RuntimeException("upload error");
		}finally{
			getPool().returnResource(jedis);
		}
	}
	// query all the processed data for a given user and stream
	@Override
	public <T> List<StreamRecord<T>> queryAll(OhmageStream stream, OhmageUser user, Class<T> dataType) {
		StreamRecordFactory recFactory = new StreamRecordFactory();
		Jedis jedis = getPool().getResource();
		jedis.select(0);
		List<String> stringStream = jedis.hvals(user.toString() + stream.toString());
		List<StreamRecord<T>> records = new ArrayList<StreamRecord<T>>();

		for(String string:stringStream){
			try {
				ObjectMapper mapper = new ObjectMapper();
				records.add(recFactory.createRecord((ObjectNode) mapper.readTree(string), user, dataType));
			} catch (Exception e) {
				logger.error("Node string: {}", string);
				throw new RuntimeException(e);
			}
		}
		getPool().returnResource(jedis);
		return records;
	}
	@Override
	public <T> List<StreamRecord<T>> queryByTimeInterval(OhmageStream stream,
			OhmageUser user, Interval interval, Class<T> dataType) {
		throw new UnsupportedOperationException();
	}
	@Override
	public <T> StreamRecord<T> queryTheLatest(OhmageStream stream,
			OhmageUser user, Class<T> dataType) {
		throw new UnsupportedOperationException();
	}
	@Override
	public <T> StreamRecord<T> queryTheEarliest(OhmageStream stream,
			OhmageUser user, Class<T> dataType) {
		throw new UnsupportedOperationException();
	}
	public RedisStreamStore(String host){
		this.host = host;
	}
	public RedisStreamStore(){
		this("localhost");
	}
}
