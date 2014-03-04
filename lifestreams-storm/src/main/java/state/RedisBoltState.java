package state;

import java.util.HashMap;
import java.util.Map;
import backtype.storm.task.TopologyContext;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.ohmage.models.OhmageUser;
import lifestreams.utils.KryoSerializer;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisBoltState {
	String key;
	// Kryo kryo ;
	static JedisPool jedisPool = new JedisPool(new JedisPoolConfig(),
			"localhost");
	HashMap<OhmageUser, UserState> internalMap = new HashMap<OhmageUser, UserState>();
	Output ob = new Output(1 * 1024 * 1024);

	public RedisBoltState(TopologyContext context) {
		super();
		// init properties
		this.key = context.getThisComponentId() + "-" + context.getThisTaskId();
		Jedis jedis = jedisPool.getResource();

		if (jedis.exists(key)) {
			Kryo kryo = KryoSerializer.getInstance();
			Map<byte[], byte[]> prevState = jedis.hgetAll(key.getBytes());
			for (byte[] key : prevState.keySet()) {
				OhmageUser user = kryo.readObject(new Input(key),
						OhmageUser.class);
				UserState state = kryo.readObject(
						new Input(prevState.get(key)), UserState.class);
				internalMap.put(user, state);
			}
		}
		jedisPool.returnResource(jedis);
	}

	public UserState newUserState(OhmageUser user) {
		if (this.containUser(user))
			return null;

		UserState state = new UserState();
		internalMap.put(user, state);
		// sync(user);

		return state;
	}

	public void sync(OhmageUser user) {
		UserState state = get(user);

		Kryo kryo = KryoSerializer.getInstance();

		Jedis jedis = jedisPool.getResource();
		ob.clear();
		kryo.writeObject(ob, user);
		byte[] userBytes = ob.toBytes();

		ob.clear();
		kryo.writeObject(ob, state);
		byte[] stateBytes = ob.toBytes();

		jedis.hset(key.getBytes(), userBytes, stateBytes);
		jedisPool.returnResource(jedis);

	}

	public UserState get(OhmageUser user) {
		return (internalMap.get(user));
	}

	public boolean containUser(OhmageUser user) {
		return (internalMap.containsKey(user));
	}
}
