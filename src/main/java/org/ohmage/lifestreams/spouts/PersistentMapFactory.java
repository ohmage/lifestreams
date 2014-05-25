package org.ohmage.lifestreams.spouts;

import java.util.Map;

import org.ohmage.lifestreams.tasks.Task;
import org.ohmage.models.OhmageUser;

import com.esotericsoftware.kryo.Kryo;

public class PersistentMapFactory {
	final IMapStore store;
	final Kryo kryo;
	final String topologyName;
	static final String PREFIX = "lifestreams.";
	public PersistentMapFactory(String topologyName, IMapStore s, Kryo kryo){
		store = s;
		this.kryo = kryo;
		this.topologyName = topologyName;
	}
	public <K,V> Map<K,V> getUserMap(OhmageUser user, String name, Class<K> kClass, Class<V> vClass){
		return store.getMap(PREFIX + ".map." + topologyName + ".usermap." + user.getUsername() + "." + name, kryo, kClass, vClass);
	}
	public <K,V> Map<K,V> getComponentMap(String cId, String name, Class<K> kClass, Class<V> vClass){
		return store.getMap(PREFIX + ".map." + topologyName + ".cmap." + cId + "." + name, kryo, kClass, vClass);
	}
	public <K,V> Map<K,V> getUserTaskMap(Task t, String name, Class<K> kClass, Class<V> vClass){
		return store.getMap(PREFIX + ".map." + topologyName + ".taskmap." + t.getComponentId() + "." + t.getUser().getUsername() + "." + name, kryo, kClass, vClass);
	}
	public <K,V> Map<K,V> getUserTaskMap(String cId, OhmageUser user, String name, Class<K> kClass, Class<V> vClass){
		return store.getMap(PREFIX + ".map." + topologyName + ".taskmap." + cId + "." + user.getUsername() + "." + name, kryo, kClass, vClass);
	}
	public <K,V> Map<K,V> getTopologyMap(String name, Class<K> kClass, Class<V> vClass){
		return store.getMap(PREFIX + ".map." + topologyName + ".topologymap." + name, kryo, kClass, vClass);
	}
	public <K,V> Map<K,V> getSystemWideMap(String name, Class<K> kClass, Class<V> vClass){
		return store.getMap(PREFIX + ".system_map." + name, kryo, kClass, vClass);
	}
	public void clearAll(){
		store.clearAll(PREFIX + ".map." + topologyName + ".*");
	}
}
