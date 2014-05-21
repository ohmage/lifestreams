package org.ohmage.lifestreams.spouts;

import java.util.List;
import java.util.Set;

import org.joda.time.DateTime;
import org.ohmage.lifestreams.bolts.UserTaskState;
import org.ohmage.lifestreams.tasks.Task;
import org.ohmage.models.OhmageStream;
import org.ohmage.models.OhmageUser;

import com.esotericsoftware.kryo.Kryo;

public interface IBookkeeper {
	public void setCheckPoint(String spout, OhmageUser user, DateTime time);
	public DateTime getCheckPoint(String spout, OhmageUser user);
	
	public void snapshotUserState(UserTaskState state, Kryo serializer);
	public UserTaskState recoverUserStateSnapshot(String componentId, OhmageUser user, Kryo serializer);
	
	public void putMap(String componentId, OhmageUser user, String name, Object key, Object value, Kryo serializer);
	public <T> T getMap(String componentId, OhmageUser user, String name, Object key, Kryo serializer,  Class<T> c);
	void removeFromMap(String componentId, OhmageUser user, String name,
			Object key, Kryo serializer);
	public <T> Set<T> getKeySet(String componentId, OhmageUser user, String name, Kryo serializer,  Class<T> c);
	
	
//	
//	public void addToList(String componentId, OhmageUser user, String name, Object obj, Kryo serializer);
//	Object getListIndex(String componentId, OhmageUser user, String name, int index,
//			Kryo serializer);
//	public void removeFirstFromList(String componentId, OhmageUser user, String name);
//	<T> List<T>  getList(String componentId, OhmageUser user, String name,
//			Kryo serializer, Class<T> c);
	public void clearAll();



}
