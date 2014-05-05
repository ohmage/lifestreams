package org.ohmage.lifestreams.stores;

import java.util.List;

import org.joda.time.Interval;
import org.ohmage.lifestreams.models.StreamRecord;
import org.ohmage.models.OhmageStream;
import org.ohmage.models.OhmageUser;

public interface StreamStore {
	boolean upload(OhmageStream stream, StreamRecord rec);
	<T> List<StreamRecord<T>> queryAll(OhmageStream stream, OhmageUser user, Class<T> dataType);
	<T> List<StreamRecord<T>> queryByTimeInterval(OhmageStream stream, OhmageUser user, Interval interval, Class<T> dataType);
	<T> StreamRecord<T> queryTheLatest(OhmageStream stream, OhmageUser user, Class<T> dataType);
	<T> StreamRecord<T> queryTheEarliest(OhmageStream stream, OhmageUser user, Class<T> dataType);
}
