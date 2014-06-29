package org.ohmage.lifestreams.tuples;

import backtype.storm.tuple.Tuple;
import org.joda.time.DateTime;
import org.ohmage.models.IUser;

public class GlobalCheckpointTuple extends BaseTuple {
    private DateTime checkpoint;

    public GlobalCheckpointTuple(Tuple t) {
        super(t);
    }

    public GlobalCheckpointTuple(IUser user, DateTime checkpoint) {
        super(user);
        this.checkpoint = checkpoint;
    }

    public DateTime getGlobalCheckpoint() {
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

    @Override
    public Object getMessageId() {
        // TODO Auto-generated method stub
        return null;
    }

}
