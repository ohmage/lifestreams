package org.ohmage.lifestreams.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.ohmage.lifestreams.models.StreamRecord;
import org.ohmage.models.OhmageStream;
import org.ohmage.models.OhmageUser;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class RedisStreamStore {
	private static ObjectMapper mapper = new ObjectMapper();
	static JedisPool pool = new JedisPool(new JedisPoolConfig(), "localhost");
	
	public static JedisPool getPool(){
		return pool;
	}
	public static List<ObjectNode> query(OhmageUser requestee, OhmageStream stream) throws IOException{
		Jedis jedis = pool.getResource();
		List<String> data = jedis.hvals(requestee.toString() + stream.toString());
		List<ObjectNode> json_data = new ArrayList<ObjectNode>();
		ObjectMapper mapper = new ObjectMapper();
		for(String s:data){
			json_data.add(mapper.readValue(s, ObjectNode.class));
		}
		
		pool.returnResource(jedis);
		return json_data;
	}
	public static void store(OhmageStream stream, StreamRecord rec) throws IOException{
		Jedis jedis = pool.getResource();
		String key = rec.getData().toString();
		String value = mapper.writeValueAsString(rec.toObserverDataPoint());
		jedis.hset(rec.getUser().toString() + stream.toString(), key, value);
		pool.returnResource(jedis);
	}
	
	
}
