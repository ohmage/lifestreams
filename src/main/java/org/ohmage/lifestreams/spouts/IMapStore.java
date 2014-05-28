package org.ohmage.lifestreams.spouts;

import java.util.Map;

import com.esotericsoftware.kryo.Kryo;

public interface IMapStore {
	public void clearAll(String pattern);
	<K, V> Map<K, V> getMap(String name, Kryo kryo, Class<K> kClass, Class<V> vClass);
}
