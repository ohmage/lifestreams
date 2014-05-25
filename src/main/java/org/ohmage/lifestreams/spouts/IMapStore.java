package org.ohmage.lifestreams.spouts;

import java.util.Map;
import java.util.Set;

import org.joda.time.DateTime;
import org.ohmage.lifestreams.bolts.UserTaskState;
import org.ohmage.models.OhmageUser;

import com.esotericsoftware.kryo.Kryo;

public interface IMapStore {
	public void clearAll(String pattern);
	<K, V> Map<K, V> getMap(String name, Kryo kryo, Class<K> kClass, Class<V> vClass);
}
