package org.ohmage.lifestreams.stores;

import org.joda.time.DateTime;
import org.ohmage.lifestreams.models.StreamRecord;
import org.ohmage.models.IStream;
import org.ohmage.models.IUser;
import org.ohmage.models.SortOrder;

import java.io.Serializable;

public interface IStreamStore extends Serializable {
    void upload(IStream stream, StreamRecord rec);

    <T> Iterable<StreamRecord<T>>  query(IStream stream, IUser user, DateTime start,
                             DateTime end, SortOrder order, int maxRows, Class<T> c);

}
