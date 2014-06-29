package org.ohmage.lifestreams.stores;

import org.joda.time.DateTime;
import org.ohmage.lifestreams.models.StreamRecord;
import org.ohmage.models.IStream;
import org.ohmage.models.IUser;
import org.ohmage.sdk.Ohmage20StreamIterator;

import java.util.List;

public class OhmageStreamStore implements IStreamStore {

    @Override
    public void upload(IStream stream, StreamRecord rec) {
        // TODO Auto-generated method stub
    }

    @Override
    public List<StreamRecord> query(IStream stream, IUser user, DateTime start, DateTime end, Ohmage20StreamIterator.SortOrder order, int maxRows) {
        return null;
    }


}
