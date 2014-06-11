package org.ohmage.lifestreams.stores;

import com.esotericsoftware.kryo.Kryo;
import org.apache.commons.lang3.StringUtils;
import org.jboss.netty.util.internal.StringUtil;
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
import java.util.Arrays;
import java.util.List;
import java.util.Set;


public class RedisStreamStore implements IStreamStore {

	private static final Logger logger = LoggerFactory.getLogger(RedisStreamStore.class);
	private static final String PREFIX = "lifestreams.stream.";
    private String host = "localhost";
    private JedisPoolConfig config = new JedisPoolConfig();
    private int DBIndex = 0;
	transient private JedisPool pool;
	JedisPool getPool(){
		if(pool == null){
			pool = new JedisPool(config, host);
		}
		return pool;
	}
    String getKeyForStream(OhmageStream stream, OhmageUser user){
        return PREFIX + StringUtils.join(
                Arrays.asList(user.getUsername(),
                              stream.getObserverId(), stream.getObserverVer(),
                              stream.getStreamId(),   stream.getStreamVer()), '/');
    }
	// store a record to the Redis server.
	@Override
	public void upload(OhmageStream stream, StreamRecord rec) {
        String key = getKeyForStream(stream, rec.getUser());
        Jedis jedis = getPool().getResource();
        jedis.select(DBIndex);
        try {
            byte[] bytes = KryoSerializer.getBytes(rec, KryoSerializer.getInstance());
            // use record timestamp as score
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
        String key = getKeyForStream(stream, user);
        Jedis jedis = getPool().getResource();
        jedis.select(DBIndex);
        double startScore = start != null ? start.getMillis() : Double.NEGATIVE_INFINITY;
        double endScore = end != null ? end.getMillis() : Double.POSITIVE_INFINITY;
        Set<byte[]> storedRecs = null;
        try {
            if(order == OhmageStreamIterator.SortOrder.Chronological) {
                if (maxRows > 0) {
                    storedRecs = jedis.zrangeByScore(key.getBytes(), startScore, endScore, 0, maxRows);
                } else{
                    storedRecs = jedis.zrangeByScore(key.getBytes(), startScore, endScore);
                }
            }else{
                if (maxRows > 0) {
                    storedRecs = jedis.zrevrangeByScore(key.getBytes(), startScore, endScore, 0, maxRows);
                } else{
                    storedRecs = jedis.zrevrangeByScore(key.getBytes(), startScore, endScore);
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
            for (byte[] stored : storedRecs) {
                StreamRecord rec = KryoSerializer.toObject(stored, StreamRecord.class, kryo);
                ret.add(rec);
            }
            return ret;
        }
        return null;
    }
    public RedisStreamStore(String host, JedisPoolConfig config, int DBIndex){
        this.host = host;
        this.config = config;
        this.DBIndex = DBIndex;
    }
	public RedisStreamStore(String host){
		this(host, new JedisPoolConfig(), 0);
	}
	public RedisStreamStore(){
		this("localhost");
	}

}
