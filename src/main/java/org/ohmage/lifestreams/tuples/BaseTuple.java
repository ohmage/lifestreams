package org.ohmage.lifestreams.tuples;

import org.ohmage.models.OhmageUser;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

abstract public class BaseTuple {

	

	protected final OhmageUser user;
	protected Tuple tuple;
	
	public static Fields getFields() {
		return new Fields( "class", "user", "1", "2");
	}

	public OhmageUser getUser() {
		return user;
	}

	public Tuple getTuple() {
		return tuple;
	}
	public BaseTuple(Tuple t) {
		this.tuple = t;
		this.user = (OhmageUser) t.getValue(1);
		this.setField1(t.getValue(2));
		this.setField2(t.getValue(3));
	}
	static public BaseTuple createFromRawTuple(Tuple t){
		Class<? extends BaseTuple> c = (Class)t.getValue(0);
		try {
			return c.getConstructor(Tuple.class).newInstance(t);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	public BaseTuple(OhmageUser user) {
		this.user = user;
	}
	public GlobalStreamId getSource() {
		return tuple.getSourceGlobalStreamid();
	}

	abstract public Object getMessageId();
	abstract protected Object getUniqueId();
	abstract protected Object getField1();
	abstract protected Object getField2();
	abstract protected void setField1(Object f1);
	abstract protected void setField2(Object f2);
	public Values getValues() {
		return new Values(this.getClass(),
						  this.getUser(), 
						  this.getField1(), 
						  this.getField2());
	}

}