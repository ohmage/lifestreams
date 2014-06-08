package org.ohmage.lifestreams.stores;

import com.esotericsoftware.kryo.Kryo;
import org.joda.time.DateTime;
import org.ohmage.lifestreams.models.StreamRecord;
import org.ohmage.lifestreams.utils.KryoSerializer;
import org.ohmage.models.OhmageStream;
import org.ohmage.models.OhmageUser;
import org.ohmage.sdk.OhmageStreamIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisException;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;


public class RedisStreamStore implements IStreamStore {

	private static final Logger logger = LoggerFactory.getLogger(RedisStreamStore.class);
	private static final String PREFIX = "lifestreams.stream.";
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
        String key = PREFIX + stream.getObserverId() + "." + stream.getStreamId() + "." + rec.getUser().getUsername();
        Jedis jedis = getPool().getResource();
        try {
            byte[] bytes = KryoSerializer.getBytes(rec, KryoSerializer.getInstance());
            double score = rec.getTimestamp().getMillis();
            jedis.zadd(key.getBytes(), score, bytes);
        } catch (JedisException e) {
            getPool().returnBrokenResource(jedis);
            jedis = null;
            logger.error("getRedisBatchResult::error::JedisException::e=" + e.toString());
            throw e;
        } catch (UnknownError e) {
            logger.error("getRedisBatchResult::error::UnknownError::e=" + e.toString());
        } finally {
            if (jedis != null) {
                getPool().returnResource(jedis);
            }
        }
	}
    @Override
    public List<StreamRecord> query(OhmageStream stream,
                                           OhmageUser user,
                                           DateTime start, DateTime end,
                                           OhmageStreamIterator.SortOrder order,
                                           int maxRows) {
        Kryo kryo = KryoSerializer.getInstance();
        String key = PREFIX + stream.getObserverId() + "." + stream.getStreamId() + "." + user.getUsername();
        Jedis jedis = getPool().getResource();
        double startScore = start != null ? start.getMillis() : Double.NEGATIVE_INFINITY;
        double endScore = end != null ? end.getMillis() : Double.POSITIVE_INFINITY;
        Set<String> storedRecs = null;
        try {
            if(order == OhmageStreamIterator.SortOrder.Chronological) {
                if (maxRows > 0) {
                    storedRecs = jedis.zrangeByScore(key, startScore, endScore, 0, maxRows);
                } else{
                    storedRecs = jedis.zrangeByScore(key, startScore, endScore);
                }
            }else{
                if (maxRows > 0) {
                    storedRecs = jedis.zrevrangeByScore(key, startScore, endScore, 0, maxRows);
                } else{
                    storedRecs = jedis.zrevrangeByScore(key, startScore, endScore);
                }
            }
        } catch (JedisException e) {
            getPool().returnBrokenResource(jedis);
            jedis = null;
            logger.error("getRedisBatchResult::error::JedisException::e=" + e.toString());
            throw e;
        } catch (UnknownError e) {
            logger.error("getRedisBatchResult::error::UnknownError::e=" + e.toString());
        } finally {
            if (jedis != null) {
                getPool().returnResource(jedis);
            }
        }

        if(storedRecs != null) {
            List<StreamRecord> ret = new ArrayList<StreamRecord>(storedRecs.size());
            for (String stored : storedRecs) {
                StreamRecord rec = KryoSerializer.toObject(stored.getBytes(), StreamRecord.class, kryo);
                ret.add(rec);
            }
            return ret;
        }
        return null;

    }

	public RedisStreamStore(String host){
		this.host = host;
	}
	public RedisStreamStore(){
		this("localhost");
	}
}
