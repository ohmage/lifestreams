package org.ohmage.lifestreams.tasks;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.ohmage.lifestreams.spouts.IBookkeeper;
import org.ohmage.lifestreams.utils.KryoSerializer;
import org.ohmage.models.OhmageUser;

import com.esotericsoftware.kryo.Kryo;


public class TwoLayeredCacheMap<KEY, VALUE> implements Map<KEY, VALUE>{
	private Map<KEY, VALUE> localCache = new HashMap<KEY, VALUE>();
	final IBookkeeper bookkeeper;
	final String cId;
	final String name;
	final OhmageUser user;
	final Kryo serializer;
	final Class<KEY> keyClass;
	final Class<VALUE> valClass;
	public TwoLayeredCacheMap(String name, Class<KEY> keyClass, Class<VALUE> valClass, OhmageUser user, String componentId, IBookkeeper bookkeeper, Kryo serializer){
		this.bookkeeper = bookkeeper;
		this.cId = componentId;
		this.name = name;
		this.user = user;
		this.serializer = serializer;
		this.keyClass = keyClass;
		this.valClass = valClass;
	}
	@Override
	public void clear() {
		localCache.clear();
		// TODO support clear more natively
		for(KEY key:bookkeeper.getKeySet(cId, user, name, serializer, keyClass)){
			bookkeeper.removeFromMap(cId, user, name, key, serializer);
		}
		
	}

	@Override
	public boolean containsKey(Object key) {
		boolean ret = false;
		if(localCache.containsKey(key)){
			return true;
		}else if(bookkeeper.getKeySet(cId, user, name, serializer, keyClass).contains(key)){
			return true;
		}
		return false;
	}

	@Override
	public boolean containsValue(Object value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Set<java.util.Map.Entry<KEY, VALUE>> entrySet() {
		throw new UnsupportedOperationException();

	}

	@Override
	public VALUE get(Object key) {
		if(localCache.containsKey(key)){
			return localCache.get(key);
		}
		VALUE val = bookkeeper.getMap(cId, user, name, key, serializer, valClass);
		if(val != null){
			return val;
		}
		return null;
	}

	@Override
	public boolean isEmpty() {
		return keySet().isEmpty();
	}

	@Override
	public Set<KEY> keySet() {
		Set<KEY> finalSet = new HashSet<KEY>();
		finalSet.addAll(localCache.keySet());
		Set<KEY> persistentSet = bookkeeper.getKeySet(cId, user, name, serializer, keyClass);
		if(persistentSet != null){
			finalSet.addAll(persistentSet);
		}
		return finalSet;
	}

	@Override
	public VALUE put(KEY key, VALUE value) {
		localCache.put(key, value);
		return null;
	}

	@Override
	public void putAll(Map<? extends KEY, ? extends VALUE> m) {
		throw new UnsupportedOperationException();
		
	}

	@Override
	public VALUE remove(Object key) {
		VALUE val = localCache.remove(key);
		if(val == null){
			bookkeeper.removeFromMap(cId, user, name, key, serializer);
		}
		return val;
	}

	@Override
	public int size() {
		return keySet().size();
	}

	@Override
	public Collection<VALUE> values() {
		throw new UnsupportedOperationException();
	}
	
	public void flushToPersistentStore(){
		for(KEY key: localCache.keySet()){
			bookkeeper.putMap(cId, user, name, key, localCache.get(key), serializer);
		}
		localCache.clear();
	}
	public void clearLocalCache(){
		localCache.clear();
	}
	

}
