package org.ohmage.lifestreams.stores;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mongodb.*;
import org.joda.time.DateTime;
import org.ohmage.lifestreams.models.StreamRecord;
import org.ohmage.models.IStream;
import org.ohmage.models.IUser;
import org.ohmage.models.SortOrder;
import org.ohmage.sdk.Ohmage20StreamIterator;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A stream store implementation based on Mongodb
 * Created by changun on 6/25/14.
 */
public class MongoStreamStore implements IStreamStore {
    transient private MongoClient _client;
    private String host = "localhost";
    StreamRecord.StreamRecordFactory dataPointFactory = new StreamRecord.StreamRecordFactory();
    public MongoClient getClient() {
        if (_client == null) {
            try {
                _client = new MongoClient(host);
            } catch (UnknownHostException e) {
                throw new RuntimeException(e);
            }
        }
        return _client;

    }

    @Override
    public void upload(IStream stream, StreamRecord rec) {
        DB db = getClient().getDB("stream");
        DBCollection coll = db.getCollection(rec.getUser().getId() + ":" + stream.toString());
        DBObject dbo = StreamRecord.getObjectMapper().convertValue(rec, BasicDBObject.class);
        // use timestamp as the primary key
        dbo.put("_id", rec.getTimestamp().getMillis());
        coll.save(dbo);

    }

    @Override
    public <T>List<StreamRecord<T>> query(IStream stream, IUser user, DateTime start, DateTime end,
                                     SortOrder order, int maxRows, Class<T> c) {
        DB db = getClient().getDB("stream");
        if (!db.collectionExists(user.getId() + ":" + stream.toString())) {
            return Collections.emptyList();
        }
        DBCollection coll = db.getCollection(user.getId() + ":" + stream.toString());
        BasicDBObject query = new BasicDBObject();
        BasicDBObject sort = new BasicDBObject("_id", order == SortOrder.Chronological ? 1 : -1);
        BasicDBObjectBuilder builder = BasicDBObjectBuilder.start();
        if (start != null) {
            builder.add("$gte", start.getMillis());
        }
        if (end != null) {
            builder.add("$lte", end.getMillis());
        }
        if (!builder.isEmpty())
            query.put("_id", builder.get());

        DBCursor cur = coll.find(query).sort(sort);
        ArrayList<StreamRecord<T>> ret = new ArrayList<StreamRecord<T>>(cur.size());
        try {
            for (DBObject aCur : cur) {
                    ret.add(dataPointFactory.createRecord(aCur, user, c));
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to create record", e);
        }
        return ret;
    }

    public MongoStreamStore(String host) {
        this.host = host;
    }

    public MongoStreamStore() {
    }
}
