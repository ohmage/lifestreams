package org.ohmage.lifestreams.stores;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisException;

import java.io.ByteArrayInputStream;
import java.io.Serializable;
import java.util.*;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

public class RedisMapStore implements IMapStore, Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -4830683070362165286L;
	final private String host;
	final private JedisPoolConfig config;
	
	transient private JedisPool pool;
	JedisPool getPool(){
		if(pool == null){
			pool = new JedisPool(config, host);
		}
		return pool;
	}
	
	private static final Logger logger = LoggerFactory.getLogger(RedisMapStore.class);
	public RedisMapStore(){
		this("localhost", new JedisPoolConfig());
	}
	public RedisMapStore(String host){
		this(host, new JedisPoolConfig());
	}
	
	private RedisMapStore(String host, JedisPoolConfig config){
		this.config = config;
		this.host = host;
	}
	
	
	public class RMap<K,V> implements Map<K,V>{
		public RMap(String name, Kryo kryo, Class<K> kClass, Class<V> vClass) {
			this.kryo = kryo;
			this.name = name;
			this.kClass = kClass;
			this.vClass = vClass;
		}

		final Kryo kryo;
		final String name;
		final Class<K> kClass;
		final Class<V> vClass;
		
		public byte[] getBytes(Object obj) {
	        ByteArrayOutputStream byteArrayOutputStream = 
	                new ByteArrayOutputStream(16384);
	        DeflaterOutputStream deflaterOutputStream = 
	                new DeflaterOutputStream(byteArrayOutputStream);
	    	Output valOutput = new Output(deflaterOutputStream);
	    	
			kryo.writeObject(valOutput, obj);
			valOutput.close();
			return byteArrayOutputStream.toByteArray();
		}
		public<T> T toObject(byte[] bytes, Class<T> c) {
		    Input in = new Input(new InflaterInputStream(new ByteArrayInputStream(bytes)));
			return kryo.readObject(in, c);
		}

		@Override
		public void clear() {
			Jedis jedis = getPool().getResource();
	        try {
	            jedis.del(name);
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
		public boolean containsKey(Object key) {
			Jedis jedis = getPool().getResource();
			boolean ret = false;
	        try {
	            ret = jedis.hexists(name.getBytes(), getBytes(key));
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
            return ret;
		}

		@Override
		public boolean containsValue(Object value) {
			throw new UnsupportedOperationException();
		}

		@Override
		public Set<java.util.Map.Entry<K, V>> entrySet() {
			Jedis jedis = getPool().getResource();
	        try {
	        	 HashMap<K, V> ret = new HashMap<K, V>();
	             for(Entry<byte[], byte[]> entry : jedis.hgetAll(name.getBytes()).entrySet()){
	            	 ret.put(toObject(entry.getKey(), kClass), toObject(entry.getValue(), vClass));
	             }
	             return ret.entrySet();
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
            return null;
		}

		@Override
		public V get(Object key) {
			Jedis jedis = getPool().getResource();
	        try {
	             byte[] bytes = jedis.hget(name.getBytes(), getBytes(key));
	             if(bytes != null){
	            	 return toObject(bytes, vClass);
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
            return null;
		}

		@Override
		public boolean isEmpty() {
			Jedis jedis = getPool().getResource();
	        try {
	             return jedis.hlen(name) > 0;
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
            return true;
		}

		@Override
		public Set<K> keySet() {
			Jedis jedis = getPool().getResource();
	        try {
	        	 HashSet<K> ret = new HashSet<K>();
	             for(byte[] entry : jedis.hkeys(name.getBytes())){
	            	 ret.add(toObject(entry, kClass));
	             }
	             return ret;
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
            return null;
		}

		@Override
		public V put(K key, V value) {
			Jedis jedis = getPool().getResource();
	        try {
	        	 jedis.hset(name.getBytes(), getBytes(key), getBytes(value));
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
            return null;
		}

		@Override
		public void putAll(Map<? extends K, ? extends V> m) {
			Jedis jedis = getPool().getResource();
	        try {
	        	 HashMap<byte[], byte[]> hash = new HashMap<byte[], byte[]> ();
	        	 for(Entry<? extends K, ? extends V> entry: m.entrySet()){
	        		 hash.put(getBytes(entry.getKey()), getBytes(entry.getValue()));
	        	 }
	        	 if(hash.size() > 0){
	        		 jedis.hmset(name.getBytes(), hash);
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
		}

		@Override
		public V remove(Object key) {
			Jedis jedis = getPool().getResource();
	        try {
	        	 jedis.hdel(name.getBytes(), getBytes(key));
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
	        return null;
		}

		@Override
		public int size() {
			Jedis jedis = getPool().getResource();
	        try {
	             return jedis.hlen(name).intValue();
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
            return 0;
		}

		@Override
		public Collection<V> values() {
			Jedis jedis = getPool().getResource();
	        try {
	        	 ArrayList<V> ret = new ArrayList<V>();
	             for(byte[] entry : jedis.hvals(name.getBytes())){
	            	 ret.add(toObject(entry, vClass));
	             }
	             return ret;
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
            return null;
		}

	
		
	}

	@Override
	public void clearAll(String pattern) {
		Jedis jedis = getPool().getResource();
        try {
        	 String keys[] = jedis.keys(pattern).toArray(new String[0]);
        	 if(keys.length > 0){
        		 jedis.del(keys);
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
	}

	@Override
	public <K, V> Map<K, V> getMap(String name, Kryo kryo, Class<K> kClass,
			Class<V> vClass) {
		return new RMap(name, kryo, kClass, vClass);
	}
}
