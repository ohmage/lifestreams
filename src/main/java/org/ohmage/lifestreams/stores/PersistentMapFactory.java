package org.ohmage.lifestreams.stores;

import com.esotericsoftware.kryo.Kryo;
import org.ohmage.lifestreams.tasks.Task;
import org.ohmage.models.IUser;

import java.util.Map;

@SuppressWarnings("SameParameterValue")
public class PersistentMapFactory {
    private final IMapStore store;
    private final Kryo kryo;
    private final String topologyName;
    private static final String PREFIX = "lifestreams";

    public PersistentMapFactory(String topologyName, IMapStore s, Kryo kryo) {
        store = s;
        this.kryo = kryo;
        this.topologyName = topologyName;
    }

    private String getBaseName() {
        return PREFIX + ".map." + topologyName;
    }

    public <K, V> Map<K, V> getUserMap(IUser user, String name, Class<K> kClass, Class<V> vClass) {
        return store.getMap(getBaseName() + ".userMap." + user.getId() + "." + name, kryo, kClass, vClass);
    }

    public <K, V> Map<K, V> getComponentMap(String cId, String name, Class<K> kClass, Class<V> vClass) {
        return store.getMap(getBaseName() + ".componentMap." + cId + "." + name, kryo, kClass, vClass);
    }

    public <K, V> Map<K, V> getUserTaskMap(Task t, String name, Class<K> kClass, Class<V> vClass) {
        return store.getMap(getBaseName() + ".taskMap." + t.getComponentId() + "." + t.getUser().getId() + "." + name, kryo, kClass, vClass);
    }

    public <K, V> Map<K, V> getUserTaskMap(String cId, IUser user, String name, Class<K> kClass, Class<V> vClass) {
        return store.getMap(getBaseName() + ".taskMap." + cId + "." + user.getId() + "." + name, kryo, kClass, vClass);
    }

    public <K, V> Map<K, V> getTopologyMap(String name, Class<K> kClass, Class<V> vClass) {
        return store.getMap(getBaseName() + ".topologyMap." + name, kryo, kClass, vClass);
    }

    public <K, V> Map<K, V> getSystemWideMap(String name, Class<K> kClass, Class<V> vClass) {
        return store.getMap(getBaseName() + ".systemMap." + name, kryo, kClass, vClass);
    }

    public void clearAll() {
        store.clearAll(getBaseName() + ".*");
    }
}
