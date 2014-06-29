package org.ohmage.lifestreams.stores;

import com.esotericsoftware.kryo.Kryo;

import java.io.Serializable;
import java.util.Map;

public interface IMapStore extends Serializable {
    public void clearAll(String pattern);

    <K, V> Map<K, V> getMap(String name, Kryo kryo, Class<K> kClass, Class<V> vClass);
}
