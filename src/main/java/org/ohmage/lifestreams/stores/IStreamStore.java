package org.ohmage.lifestreams.stores;

import org.joda.time.DateTime;
import org.ohmage.lifestreams.models.StreamRecord;
import org.ohmage.models.OhmageStream;
import org.ohmage.models.OhmageUser;
import org.ohmage.sdk.OhmageStreamIterator;

import java.io.Serializable;
import java.util.List;

public interface IStreamStore extends Serializable {
	void upload(OhmageStream stream, StreamRecord rec);
	List<StreamRecord> query(OhmageStream stream, OhmageUser user, DateTime start,
                                    DateTime end, OhmageStreamIterator.SortOrder order, int maxRows);
}
