package org.ohmage.lifestreams.stores;

import java.util.ArrayList;
import java.util.List;

import org.joda.time.Interval;
import org.ohmage.lifestreams.models.StreamRecord;
import org.ohmage.lifestreams.models.StreamRecord.StreamRecordFactory;
import org.ohmage.models.OhmageStream;
import org.ohmage.models.OhmageUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

@Component
public class RedisStreamStore implements StreamStore {
	private ObjectMapper mapper = new ObjectMapper();
	private Logger logger = LoggerFactory.getLogger(RedisStreamStore.class);
	@Value("${redis.host}")
	String host;

	transient private JedisPool pool;
	public JedisPool getPool(){
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
		StreamRecordFactory<T> recFactory = StreamRecordFactory.createStreamRecordFactory(dataType);
		Jedis jedis = getPool().getResource();
		jedis.select(0);
		List<String> stringStream = jedis.hvals(user.toString() + stream.toString());
		List<StreamRecord<T>> records = new ArrayList<StreamRecord<T>>();

		for(String string:stringStream){
			try {
				records.add(recFactory.createRecord((ObjectNode) mapper.readTree(string), user));
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
}
