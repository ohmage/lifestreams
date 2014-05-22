package org.ohmage.lifestreams.tuples;

import org.joda.time.DateTime;
import org.ohmage.lifestreams.models.StreamRecord;
import org.ohmage.models.OhmageUser;

import backtype.storm.tuple.Tuple;

public class SpoutRecordTuple extends RecordTuple{
	public static class RecordTupleMsgId implements Comparable{
		final OhmageUser user;
		final DateTime time;
		public RecordTupleMsgId(long batchId, long serialId, DateTime time, OhmageUser user) {
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
		public OhmageUser getUser() {
			return user;
		}
		public DateTime getTime() {
			return time;
		}
		final long batchId, serialId;
		@Override
		public int compareTo(Object arg0) {
			// TODO Auto-generated method stub
			return (int) (getSerialId() - ((RecordTupleMsgId)arg0).getSerialId());
		}

	}
	long batchId, serialId;
	public SpoutRecordTuple(Tuple t) {
		super(t);
	}
	public SpoutRecordTuple(StreamRecord rec, long batchId, long serialId){
		super(rec);
		this.batchId = batchId;
		this.serialId = serialId;
	}
	@Override
	public Object getMessageId() {
		return new RecordTupleMsgId(batchId, serialId, this.getTimestamp(), getUser());
	}

}
