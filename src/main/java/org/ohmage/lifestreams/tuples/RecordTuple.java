package org.ohmage.lifestreams.tuples;

import org.joda.time.DateTime;
import org.ohmage.lifestreams.models.StreamRecord;

import backtype.storm.tuple.Tuple;
@SuppressWarnings("rawtypes")
public class RecordTuple extends BaseTuple {
	protected StreamRecord rec;

	public RecordTuple(Tuple t){
		super(t);
	}
	public RecordTuple(StreamRecord rec) {
		super(rec.getUser());
		this.rec = rec;
	}
	public StreamRecord getStreamRecord() {
		return rec;
	}
	public DateTime getTimestamp(){
		return rec.getTimestamp();
	}

	@Override
	protected Object getUniqueId() {
		return rec.getTimestamp();
	}
	@Override
	protected Object getField1() {
		return rec;
	}
	@Override
	protected Object getField2() {
		return null;
	}
	@Override
	protected void setField1(Object f1) {
		this.rec = (StreamRecord) f1;
	}
	@Override
	protected void setField2(Object f2) {
	}
	@Override
	public Object getMessageId() {
		return null;
	}
	
}
