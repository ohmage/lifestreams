package org.ohmage.lifestreams.spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.joda.time.DateTime;
import org.ohmage.lifestreams.models.StreamRecord;
import org.ohmage.lifestreams.models.StreamRecord.StreamRecordFactory;
import org.ohmage.lifestreams.tuples.RecordTuple;
import org.ohmage.models.IUser;
import org.ohmage.models.Ohmage20Stream;
import org.ohmage.models.Ohmage20User;
import org.ohmage.sdk.Ohmage20StreamClient;
import org.ohmage.sdk.Ohmage20StreamIterator;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @param <T> the output data type. This class must follow
 *            the schema of the "data" field of the ohmage stream, or it can
 *            be the Jacksson "ObjectNode".
 * @author changun
 */
public class Ohmage20StreamSpout<T> extends Ohmage20Spout<T> {
    // stream to query
    private Ohmage20Stream stream;
    // data point factory
    private StreamRecordFactory factory;
    // the columns to be queried
    private String columnList;
    // the Type of the emitted records
    private Class<T> c;

    @Override
    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {

        super.open(conf, context, collector);
        // create data point factory
        this.factory = new StreamRecordFactory();

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(RecordTuple.getFields());

    }


    /**
     * @param requester  the ohmage user of which we query data in behalf
     * @param since      the start date of the query
     * @param c          the class of the returned data point. This class must follow
     *                   the schema of the "data" field of the ohmage stream, or it can
     *                   be the Jackason "ObjectNode".
     * @param stream     the ohmage stream to be queried
     *                   a list of ohmage users we will get the data from
     * @param columnList the columns to be returned. set it null means return every column
     */
    public Ohmage20StreamSpout(Ohmage20User requester, DateTime since, Class<T> c, Ohmage20Stream stream,
                               String columnList) {
        super(requester, since, 10, TimeUnit.MINUTES);
        this.c = c;
        this.stream = stream;
        this.columnList = columnList;
    }


    @Override
    protected Iterator<StreamRecord<T>> getIteratorFor(final IUser user,
                                                       DateTime since) {
        logger.trace("Fetch data for user {} from {}({}) since {}!", user.getId(),
                stream.getObserverId(),
                stream.getId(),
                since);
        try {
            final Ohmage20StreamIterator iter = new Ohmage20StreamClient(getRequester())
                    .getOhmageStreamIteratorBuilder(stream, user)
                    .startDate(since.minusDays(1)) // to deal with ohmage stream api bug
                    .columnList(columnList)
                    .build();

            // move the iterator pointer to the location right before the record whose timestamp >= since
            final PeekingIterator<ObjectNode> peekableIter = new PeekingIterator<ObjectNode>(iter);
            while (iter.hasNext()) {
                StreamRecord<T> rec = factory.createRecord(peekableIter.peek(), user, c);
                if (rec.getTimestamp().compareTo(since) >= 0) {
                    break;
                } else {
                    peekableIter.next();
                }
            }
            return new ICloseableIterator<StreamRecord<T>>() {
                @Override
                public boolean hasNext() {
                    return peekableIter.hasNext();

                }

                @Override
                public StreamRecord<T> next() {
                    ObjectNode json = peekableIter.next();
                    try {
                        return factory.createRecord(json, user, c);

                    } catch (Exception e) {
                        logger.error("convert ohmage record error", e);
                        throw new RuntimeException(e);
                    }
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();

                }

                @Override
                public void close() throws IOException {
                    iter.close();
                }

            };

        } catch (Exception e) {
            logger.error("Fetch data error for user {} from {}({}) since {}!", user.getId(),
                    stream.getObserverId(),
                    stream.getId(),
                    since);
            logger.error("Trace: ", e);
        }
        return null;
    }
}
