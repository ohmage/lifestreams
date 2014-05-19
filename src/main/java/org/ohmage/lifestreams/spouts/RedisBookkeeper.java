package org.ohmage.lifestreams.spouts;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.joda.time.DateTime;
import org.ohmage.lifestreams.bolts.UserState;
import org.ohmage.lifestreams.stores.RedisStreamStore;
import org.ohmage.lifestreams.tasks.Task;
import org.ohmage.lifestreams.utils.KryoSerializer;
import org.ohmage.models.OhmageStream;
import org.ohmage.models.OhmageUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import redis.clients.jedis.Jedis;

@Component
public class RedisBookkeeper implements IBookkeeper, Serializable {
	private static final String PREFIX = "lifestreams.";

	private static final String CHECKPOINT = PREFIX + "checkpoint";
	private static final String SNAPSHOT = PREFIX + "snapshot";
	private static final String MAP = PREFIX + "maps.";
	
	@Autowired
	RedisStreamStore redisStore;
	
	private Logger logger = LoggerFactory.getLogger(RedisBookkeeper.class);
	
	private void setTimestamp(String key, String stream, OhmageUser user,
			DateTime time){
		Jedis jedis = redisStore.getPool().getResource();
		if(time != null){
			jedis.hset(key, "" + stream + user, time.toString());
		}else{
			jedis.hdel(key, "" + stream + user);
		}
		redisStore.getPool().returnResource(jedis);
	}
	private DateTime getTimestamp(String key, String stream,
			OhmageUser user) {
		
		Jedis jedis = redisStore.getPool().getResource();
		String ret = jedis.hget(key, "" + stream + user);
		redisStore.getPool().returnResource(jedis);
		if(ret != null){
			return new DateTime(ret);
		}else{
			return null;
		}
	}
	public void clearAll(){
		LoggerFactory.getLogger(RedisBookkeeper.class).info("Clear the state of the previous process");
		Jedis jedis = redisStore.getPool().getResource();
		for(String key: jedis.keys(PREFIX + "*")){
			jedis.del(key);
		}
		redisStore.getPool().returnResource(jedis);
	}
	@Override
	public void setCheckPoint(String spout, OhmageUser user,
			DateTime time) {
		setTimestamp(CHECKPOINT, spout, user, time);
		
	}
	@Override
	public DateTime getCheckPoint(String spout, OhmageUser user) {
		return getTimestamp(CHECKPOINT, spout, user);
	}
	@Override
	public void snapshotUserState(UserState state, Kryo serializer) {
		
		Jedis jedis = redisStore.getPool().getResource();
		Output output = new Output(1024, 1024*1024*3);
		serializer.writeClassAndObject(output, state);
		//logger.info("Make a snapshot of {} of {} KB", state.getTask().getClass(), output.total() / 1024.0);
		byte[] hashkey = (state.getKey()).getBytes();
		jedis.hset(SNAPSHOT.getBytes(), hashkey, output.getBuffer());
		output.close();
		redisStore.getPool().returnResource(jedis);
		// test
		//recoverUserStateSnapshot(state.getBolt().getComponentId(), state.getUser(), serializer) ;
		
	}
	@Override
	public UserState recoverUserStateSnapshot(String componentId, OhmageUser user, Kryo serializer) {
		Jedis jedis = redisStore.getPool().getResource();
		UserState state = null;
		String key = UserState.getKey(componentId, user);
		byte[] bytes = jedis.hget(SNAPSHOT.getBytes(), key.getBytes());
		if(bytes != null){
		
			Input input = new Input(bytes);
			state = (UserState) serializer.readClassAndObject(input);
			logger.info("Recover a snapshot of {} of {} KB", state.getTask().getClass(), bytes.length / 1024.0);
			input.close();
		}
		redisStore.getPool().returnResource(jedis);
		return state;
	}
	@Override
	public void putMap(String componentId, OhmageUser user, String name,
			Object key, Object value, Kryo serializer) {
		Jedis jedis = redisStore.getPool().getResource();
		Output keyOutput = new Output(1024, 1024*1024*3);
		
		serializer.writeClassAndObject(keyOutput, key);
		

		
        ByteArrayOutputStream byteArrayOutputStream = 
                new ByteArrayOutputStream(16384);
            DeflaterOutputStream deflaterOutputStream = 
                new DeflaterOutputStream(byteArrayOutputStream);
    	Output valOutput = new Output(deflaterOutputStream);
    	
		serializer.writeClassAndObject(valOutput, value);
		valOutput.close();
		//logger.info("put to {} key {} size {} KB", componentId, key.toString(), byteArrayOutputStream.toByteArray().length / 1024.0);
		jedis.hset((MAP + componentId + user).getBytes(), keyOutput.getBuffer(), byteArrayOutputStream.toByteArray());
		redisStore.getPool().returnResource(jedis);
	}
	@Override
	public void removeFromMap(String componentId, OhmageUser user, String name,
			Object key, Kryo serializer) {
		Jedis jedis = redisStore.getPool().getResource();
		Output keyOutput = new Output(1024, 1024*1024*3);
		serializer.writeClassAndObject(keyOutput, key);
		jedis.hdel((MAP + componentId + user).getBytes(), keyOutput.getBuffer());
		redisStore.getPool().returnResource(jedis);
	}
	@Override
	public <T> T getMap(String componentId, OhmageUser user, String name,
			Object key, Kryo serializer, Class<T> c) {
		Jedis jedis = redisStore.getPool().getResource();
		T obj = null;
		Output keyOutput = new Output(1024, 1024*1024*3);
		serializer.writeClassAndObject(keyOutput, key);
		byte[] bytes = jedis.hget((MAP + componentId + user).getBytes(), keyOutput.getBuffer());
		
		if(bytes != null){
		    Input in = new Input(new InflaterInputStream(new ByteArrayInputStream(bytes)));
			obj = (T)serializer.readClassAndObject(in);
		}
		redisStore.getPool().returnResource(jedis);
		return obj; 
	}
	@Override
	public <T> Set<T> getKeySet(String componentId, OhmageUser user,
			String name, Kryo serializer, Class<T> c) {
		Jedis jedis = redisStore.getPool().getResource();
		Set<byte[]> keyBytes = jedis.hkeys((MAP + componentId + user).getBytes());
		Set<T> keys = new HashSet<T>();
		for(byte[] bytes: keyBytes){
			T key = (T)serializer.readClassAndObject(new Input(bytes));
			keys.add(key);
		}
		redisStore.getPool().returnResource(jedis);
		return keys;
	}

	
	
	
//	@Override
//	public void addToList(String componentId, OhmageUser user, String name,
//			Object obj, Kryo serializer) {
//		Jedis jedis = redisStore.getPool().getResource();
//		Output output = new Output(1024, 1024*1024*3);
//		serializer.writeClassAndObject(output, obj);
//		jedis.rpush((LIST + componentId + user).getBytes(), output.getBuffer());
//		redisStore.getPool().returnResource(jedis);
//		
//	}
//	@Override
//	public Object getListIndex(String componentId, OhmageUser user,
//			String name, int index, Kryo serializer) {
//		Object obj = null;
//		Jedis jedis = redisStore.getPool().getResource();
//		
//		byte[] bytes = jedis.lindex((LIST + componentId + user).getBytes(), index);
//		if(bytes != null){
//			Input input = new Input(bytes);
//			obj = serializer.readClassAndObject(input);
//		}
//		redisStore.getPool().returnResource(jedis);
//		return obj;
//	}
//	@Override
//	public void removeFirstFromList(String componentId, OhmageUser user,
//			String name) {
//		Jedis jedis = redisStore.getPool().getResource();
//		jedis.lpop(LIST + componentId + user);
//		redisStore.getPool().returnResource(jedis);
//	}
//	@Override
//	public <T> List<T> getList(String componentId, OhmageUser user,
//			String name, Kryo serializer, Class<T> c) {
//		Jedis jedis = redisStore.getPool().getResource();
//		List<T> objList = null;
//		List<byte[]> objBytes = jedis.lrange((LIST + componentId + user).getBytes(), 0, -1);
//		logger.info("Recovering output cache with {} elements", objBytes.size());
//		if(objBytes != null){
//			objList = new ArrayList<T>(objBytes.size());
//			for(byte[] bytes: objBytes){
//				Input input = new Input(bytes);
//				T obj = (T)serializer.readClassAndObject(input);
//				objList.add(obj);
//			}
//		}
//		redisStore.getPool().returnResource(jedis);
//		return objList;
//	}

	
	
	

}
