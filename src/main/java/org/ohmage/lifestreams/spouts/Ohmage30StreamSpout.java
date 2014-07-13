package org.ohmage.lifestreams.spouts;

import org.joda.time.DateTime;
import org.ohmage.lifestreams.models.StreamRecord;
import org.ohmage.lifestreams.oauth.Scope;
import org.ohmage.lifestreams.stores.OhmageStreamStore;
import org.ohmage.models.IUser;
import org.ohmage.models.Ohmage30Stream;
import org.ohmage.models.SortOrder;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

/**
 * Created by changun on 6/29/14.
 */
public class Ohmage30StreamSpout<T> extends Ohmage30Spout<T>{
    final private OhmageStreamStore streamStore;
    final private Ohmage30Stream stream;
    final private Class<T> dataPointClass;
    public Ohmage30StreamSpout(OhmageStreamStore streamStore, Ohmage30Stream stream,
                               Class<T> dataPointClass, DateTime since) {
        super(new Scope("ohmage",stream.getReadScopeName()),
                streamStore.getTokenManager(),
                since,
                10,
                TimeUnit.MINUTES);
        this.streamStore = streamStore;
        this.stream = stream;
        this.dataPointClass = dataPointClass;
    }

    @Override
    protected Iterator<StreamRecord<T>> getIteratorFor(IUser user, DateTime since) {
        return streamStore.query(stream, user, since, null,
                SortOrder.Chronological, -1, dataPointClass).iterator();
    }
}
