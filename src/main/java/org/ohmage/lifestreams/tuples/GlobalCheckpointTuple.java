package org.ohmage.lifestreams.tuples;

import org.joda.time.DateTime;
import org.ohmage.models.OhmageUser;

import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class GlobalCheckpointTuple extends BaseTuple {
	private DateTime checkpoint;
	public GlobalCheckpointTuple(Tuple t){
		super(t);
	}
	public GlobalCheckpointTuple(OhmageUser user, DateTime checkpoint){
		super(user);
		this.checkpoint = checkpoint;
	}
	public DateTime getGlobalCheckpoint(){
		return checkpoint;
	}
	@Override
	protected Object getUniqueId() {
		return checkpoint;
	}
	@Override
	protected Object getField1() {
		return checkpoint;
	}
	@Override
	protected Object getField2() {
		return null;
	}
	@Override
	protected void setField1(Object f1) {
		checkpoint = (DateTime) f1;
	}
	@Override
	protected void setField2(Object f2) {
	}
}
