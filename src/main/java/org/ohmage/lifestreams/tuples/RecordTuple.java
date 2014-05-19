package org.ohmage.lifestreams.tuples;

import java.util.Arrays;
import java.util.List;

import org.joda.time.DateTime;
import org.ohmage.lifestreams.models.StreamRecord;
import org.ohmage.lifestreams.spouts.BaseOhmageSpout;
import org.ohmage.models.OhmageUser;

import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImpl;
@SuppressWarnings("rawtypes")
public class RecordTuple extends BaseTuple {
	private StreamRecord rec; 
	private Long batchId;
	public RecordTuple(Tuple t){
		super(t);
	}
	public RecordTuple(StreamRecord rec, long batchId) {
		super(rec.getUser());
		this.rec = rec;
		this.batchId = batchId;
		if(batchId  < 0){
			
			throw new RuntimeException("BatchId  must >= 0");
		}
		
	}
	public StreamRecord getStreamRecord() {
		return rec;
	}
	public DateTime getTimestamp(){
		return rec.getTimestamp();
	}
	public Long getBatchId(){
		return this.batchId;
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
		return batchId;
	}
	@Override
	protected void setField1(Object f1) {
		this.rec = (StreamRecord) f1;
	}
	@Override
	protected void setField2(Object f2) {
		this.batchId = (Long) f2;
	}
	public static DateTime getTimestampFromRawTuple(Tuple t){
		return ((StreamRecord)t.getValue(2)).getTimestamp();
	}
	
}
