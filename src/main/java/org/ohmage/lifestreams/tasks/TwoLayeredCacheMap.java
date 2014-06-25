package org.ohmage.lifestreams.tasks;

import java.util.*;


public class TwoLayeredCacheMap<KEY, VALUE> implements Map<KEY, VALUE>{
	private Map<KEY, VALUE> volatileMap = new HashMap<KEY, VALUE>();
	private Map<KEY, VALUE> persistentMapMirror = new HashMap<KEY, VALUE>();
	private final Map<KEY, VALUE> persistentMap;
	public TwoLayeredCacheMap(Map<KEY, VALUE> persistentMap){
		this.persistentMap = persistentMap;
		this.persistentMapMirror.putAll(persistentMap);
	}
	@Override
	public void clear() {
		volatileMap.clear();
		persistentMapMirror.clear();
		this.persistentMap.clear();
	}

	@Override
	public boolean containsKey(Object key) {
		if(volatileMap.containsKey(key)){
			return true;
		}else if(persistentMapMirror.containsKey(key)){
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
		if(volatileMap.containsKey(key)){
			return volatileMap.get(key);
		}
		if(persistentMapMirror.containsKey(key)){
			return persistentMapMirror.get(key);
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
		finalSet.addAll(volatileMap.keySet());
		finalSet.addAll(persistentMapMirror.keySet());
		return finalSet;
	}

	@Override
	public VALUE put(KEY key, VALUE value) {
		volatileMap.put(key, value);
		return null;
	}

	@Override
	public void putAll(Map<? extends KEY, ? extends VALUE> m) {
		volatileMap.putAll(m);
	}

	@Override
	public VALUE remove(Object key) {
		VALUE val = volatileMap.remove(key);
		if(val == null){
			val = this.persistentMapMirror.remove(key);
			if(val != null){
				this.persistentMap.remove(key);
			}
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
	
	public void persist(){
		this.persistentMap.putAll(volatileMap);
		persistentMapMirror.putAll(volatileMap);
		volatileMap.clear();
	}
	public void clearLocalCache(){
		volatileMap.clear();
	}
	

}
