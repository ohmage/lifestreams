package org.ohmage.lifestreams.stores;

import org.joda.time.DateTime;
import org.ohmage.lifestreams.models.StreamRecord;
import org.ohmage.models.OhmageStream;
import org.ohmage.models.OhmageUser;
import org.ohmage.sdk.OhmageStreamIterator;

import java.util.List;

public class OhmageStreamStore implements IStreamStore {

	@Override
	public void upload(OhmageStream stream, StreamRecord rec) {
		// TODO Auto-generated method stub
	}

    @Override
    public <T> List<StreamRecord<T>> query(OhmageStream stream, OhmageUser user, DateTime start, DateTime end, OhmageStreamIterator.SortOrder order, int maxRows, Class<T> dataType) {
        return null;
    }


}
