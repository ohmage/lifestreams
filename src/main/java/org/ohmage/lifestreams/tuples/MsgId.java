package org.ohmage.lifestreams.tuples;

import org.joda.time.DateTime;
import org.ohmage.models.OhmageUser;

public class MsgId{
	public MsgId(Class c, OhmageUser user, Object id) {
		super();
		this.c = c;
		this.user = user;
		this.id = id;
	}
	final OhmageUser user;
	final Object id;
	final Class c;
	public OhmageUser getUser() {
		return user;
	}
	public Object getId() {
		return id;
	}
	public Class getTupleClass(){
		return c;
	}
	@Override
	public int hashCode(){
		return (c.toString()+ user.getUsername() + id.toString()).hashCode();
	}
}