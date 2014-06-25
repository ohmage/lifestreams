package org.ohmage.lifestreams.stores;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mongodb.*;
import org.joda.time.DateTime;
import org.ohmage.lifestreams.models.StreamRecord;
import org.ohmage.models.OhmageStream;
import org.ohmage.models.OhmageUser;
import org.ohmage.sdk.OhmageStreamIterator;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A stream store implementation based on Mongodb
 * Created by changun on 6/25/14.
 */
public class MongoStreamStore implements  IStreamStore{
    transient private MongoClient _client;
    private String host = "localhost";
    public MongoClient getClient()  {
        if(_client == null) {
            try {
                _client = new MongoClient(host);
            } catch (UnknownHostException e) {
                throw new RuntimeException(e);
            }
        }
        return _client;

    }

    @Override
    public void upload(OhmageStream stream, StreamRecord rec) {
        DB db = getClient().getDB("stream");
        DBCollection coll = db.getCollection(rec.getUser().getUsername() + ":" + stream.toString());
        DBObject dbo = StreamRecord.getObjectMapper().convertValue(rec, BasicDBObject.class);
        // use timestamp as the primary key
        dbo.put("_id", rec.getTimestamp().getMillis());
        coll.save(dbo);

    }

    @Override
    public List<StreamRecord> query(OhmageStream stream, OhmageUser user, DateTime start, DateTime end, OhmageStreamIterator.SortOrder order, int maxRows) {
        DB db = getClient().getDB("stream");
        if(!db.collectionExists(user.getUsername() + ":" + stream.toString())){
            return Collections.emptyList();
        }
        DBCollection coll = db.getCollection(user.getUsername() + ":" + stream.toString());
        BasicDBObject query = new BasicDBObject();
        BasicDBObject sort = new BasicDBObject("_id", order == OhmageStreamIterator.SortOrder.Chronological ? 1:-1);
        BasicDBObjectBuilder builder = BasicDBObjectBuilder.start();
        if(start != null) {
            builder.add("$gte", start.getMillis());
        }
        if(end!=null){
            builder.add("$lte", end.getMillis());
        }
        if(!builder.isEmpty())
            query.put("_id", builder.get());

        DBCursor cur = coll.find(query).sort(sort);
        ArrayList<StreamRecord> ret = new ArrayList<StreamRecord>(cur.size());
        for (DBObject aCur : cur) {
            ret.add(StreamRecord.getObjectMapper().convertValue(aCur, new StreamRecord<ObjectNode>().getClass()));
        }
        return ret;
    }
    public MongoStreamStore(String host){
        this.host = host;
    }
    public MongoStreamStore(){
    }
}
