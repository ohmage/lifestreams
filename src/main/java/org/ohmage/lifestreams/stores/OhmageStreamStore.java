package org.ohmage.lifestreams.stores;

import java.util.List;

import org.joda.time.Interval;
import org.ohmage.lifestreams.models.StreamRecord;
import org.ohmage.models.OhmageStream;
import org.ohmage.models.OhmageUser;

public class OhmageStreamStore implements StreamStore {

	@Override
	public boolean upload(OhmageStream stream, StreamRecord rec) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public <T> List<StreamRecord<T>> queryAll(OhmageStream stream,
			OhmageUser user, Class<T> dataType) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> List<StreamRecord<T>> queryByTimeInterval(OhmageStream stream,
			OhmageUser user, Interval interval, Class<T> dataType) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> StreamRecord<T> queryTheLatest(OhmageStream stream,
			OhmageUser user, Class<T> dataType) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> StreamRecord<T> queryTheEarliest(OhmageStream stream,
			OhmageUser user, Class<T> dataType) {
		// TODO Auto-generated method stub
		return null;
	}

}
