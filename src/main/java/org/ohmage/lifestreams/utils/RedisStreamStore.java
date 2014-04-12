package org.ohmage.lifestreams.utils;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.ohmage.lifestreams.models.StreamRecord;
import org.ohmage.models.OhmageStream;
import org.ohmage.models.OhmageUser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

@Component
public class RedisStreamStore implements Serializable {
	private ObjectMapper mapper = new ObjectMapper();
	@Value("${redis.host}")
	String host;
	
	private JedisPool pool;
	
	public JedisPool getPool(){
		if(pool == null){
			pool = new JedisPool(new JedisPoolConfig(), host);
		}
		return pool;
	}
	public List<ObjectNode> query(OhmageUser requestee, OhmageStream stream) throws IOException{
		Jedis jedis = getPool().getResource();
		jedis.select(0);
		List<String> data = jedis.hvals(requestee.toString() + stream.toString());
		List<ObjectNode> json_data = new ArrayList<ObjectNode>();
		ObjectMapper mapper = new ObjectMapper();
		for(String s:data){
			json_data.add(mapper.readValue(s, ObjectNode.class));
		}
		getPool().returnResource(jedis);
		return json_data;
	}
	public void store(OhmageStream stream, StreamRecord rec) throws IOException{
		Jedis jedis = getPool().getResource();
		jedis.select(0);
		String key = rec.getData().toString();
		String value = mapper.writeValueAsString(rec.toObserverDataPoint());
		jedis.hset(rec.getUser().toString() + stream.toString(), key, value);
		getPool().returnResource(jedis);
	}
	
	
}
