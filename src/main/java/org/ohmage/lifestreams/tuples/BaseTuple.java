package org.ohmage.lifestreams.tuples;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.ohmage.models.OhmageUser;

abstract public class BaseTuple {

	

	private final OhmageUser user;
	private Tuple tuple;
	
	public static Fields getFields() {
		return new Fields( "class", "user", "1", "2");
	}

	public OhmageUser getUser() {
		return user;
	}

	public Tuple getTuple() {
		return tuple;
	}
	BaseTuple(Tuple t) {
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
	BaseTuple(OhmageUser user) {
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