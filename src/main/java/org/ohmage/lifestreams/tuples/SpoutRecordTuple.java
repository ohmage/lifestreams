package org.ohmage.lifestreams.tuples;

import backtype.storm.tuple.Tuple;
import org.joda.time.DateTime;
import org.ohmage.lifestreams.models.StreamRecord;
import org.ohmage.models.IUser;

public class SpoutRecordTuple extends RecordTuple {
    public static class RecordTupleMsgId implements Comparable {
        final IUser user;
        final DateTime time;

        public RecordTupleMsgId(long batchId, long serialId, DateTime time, IUser user) {
            super();
            this.batchId = batchId;
            this.serialId = serialId;
            this.user = user;
            this.time = time;
        }

        public long getBatchId() {
            return batchId;
        }

        public long getSerialId() {
            return serialId;
        }

        public IUser getUser() {
            return user;
        }

        public DateTime getTime() {
            return time;
        }

        final long batchId, serialId;

        @Override
        public int compareTo(Object arg0) {
            // TODO Auto-generated method stub
            return (int) (getSerialId() - ((RecordTupleMsgId) arg0).getSerialId());
        }

    }

    private long batchId;
    private long serialId;

    public SpoutRecordTuple(Tuple t) {
        super(t);
    }

    public SpoutRecordTuple(StreamRecord rec, long batchId, long serialId) {
        super(rec);
        this.batchId = batchId;
        this.serialId = serialId;
    }

    @Override
    public Object getMessageId() {
        return new RecordTupleMsgId(batchId, serialId, this.getTimestamp(), getUser());
    }

}
