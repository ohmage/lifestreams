package org.ohmage.lifestreams.spouts;

import com.esotericsoftware.kryo.Kryo;
import org.ohmage.lifestreams.stores.IMapStore;
import org.ohmage.lifestreams.tasks.Task;
import org.ohmage.models.OhmageUser;

import java.util.Map;

@SuppressWarnings("SameParameterValue")
public class PersistentMapFactory {
	private final IMapStore store;
	private final Kryo kryo;
	private final String topologyName;
	private static final String PREFIX = "lifestreams";
	public PersistentMapFactory(String topologyName, IMapStore s, Kryo kryo){
		store = s;
		this.kryo = kryo;
		this.topologyName = topologyName;
	}
	private String getBaseName(){
		return PREFIX + ".map." + topologyName;
	}
	public <K,V> Map<K,V> getUserMap(OhmageUser user, String name, Class<K> kClass, Class<V> vClass){
		return store.getMap(getBaseName() + ".userMap." + user.getUsername() + "." + name, kryo, kClass, vClass);
	}
	public <K,V> Map<K,V> getComponentMap(String cId, String name, Class<K> kClass, Class<V> vClass){
		return store.getMap(getBaseName() + ".componentMap." + cId + "." + name, kryo, kClass, vClass);
	}
	public <K,V> Map<K,V> getUserTaskMap(Task t, String name, Class<K> kClass, Class<V> vClass){
		return store.getMap(getBaseName() + ".taskMap." + t.getComponentId() + "." + t.getUser().getUsername() + "." + name, kryo, kClass, vClass);
	}
	public <K,V> Map<K,V> getUserTaskMap(String cId, OhmageUser user, String name, Class<K> kClass, Class<V> vClass){
		return store.getMap(getBaseName() + ".taskMap." + cId + "." + user.getUsername() + "." + name, kryo, kClass, vClass);
	}
	public <K,V> Map<K,V> getTopologyMap(String name, Class<K> kClass, Class<V> vClass){
		return store.getMap(getBaseName() + ".topologyMap." + name, kryo, kClass, vClass);
	}
	public <K,V> Map<K,V> getSystemWideMap(String name, Class<K> kClass, Class<V> vClass){
		return store.getMap(getBaseName() + ".systemMap." + name, kryo, kClass, vClass);
	}
	public void clearAll(){
		store.clearAll(getBaseName() + ".*");
	}
}
