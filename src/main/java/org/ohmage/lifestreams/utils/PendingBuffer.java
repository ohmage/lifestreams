package org.ohmage.lifestreams.utils;

import backtype.storm.generated.GlobalStreamId;
import org.ohmage.lifestreams.tuples.RecordTuple;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * This buffer store and sort the records that is not in the current time window
 */
public class PendingBuffer {
    // we keep a buffer for each data stream
    private List<RecordTuple> buffer = new LinkedList<RecordTuple>();

    private Set<GlobalStreamId> pendingStreams = new HashSet<GlobalStreamId>();

    /**
     * @param tuple record to buffer.
     */
    public void put(RecordTuple tuple) {
        pendingStreams.add(tuple.getSource());
        for (int i = 0; i < buffer.size(); i++) {
            // TODO: use priority queue
            if (tuple.getTimestamp().isBefore(buffer.get(i).getTimestamp())) {
                buffer.add(i, tuple);
                return;
            }
        }
        buffer.add(tuple);
    }

    /**
     * @return a set of streams whose records are pending
     */
    public Set<GlobalStreamId> getPendingStreams() {
        return pendingStreams;
    }

    /**
     * @return pending records sorted by time
     */
    public List<RecordTuple> getBuffer() {
        return buffer;
    }

    public void clearBuffer() {
        this.buffer.clear();
        this.pendingStreams.clear();
    }

}
