package org.ohmage.lifestreams.tuples;

import org.ohmage.models.OhmageUser;

import backtype.storm.tuple.Tuple;

public class StreamStatusTuple extends BaseTuple {
	public enum StreamStatus{
		HEAD,
		MIDDLE,
		END;
	}
	public StreamStatus getStatus() {
		return status;
	}

	public Long getBatchId() {
		return batchId;
	}

	private StreamStatus status;
	private Long batchId;
	
	public StreamStatusTuple(Tuple t){
		super(t);
	}
	public StreamStatusTuple(OhmageUser user, Long batchId, StreamStatus status) {
		super(user);
		this.status = status;
		this.batchId = batchId;
	}

	@Override
	protected Object getUniqueId() {
		return "" + this.status + this.batchId;
	}

	@Override
	protected Object getField1() {
		return status;
	}

	@Override
	protected Object getField2() {
		return batchId;
	}

	@Override
	protected void setField1(Object f1) {
		this.status = (StreamStatus) f1;
	}

	@Override
	protected void setField2(Object f2) {
		this.batchId = (Long) f2;
	}

	@Override
	public Object getMessageId() {
		return null;
	}

}
