package org.ohmage.lifestreams.spouts;

import backtype.storm.topology.OutputFieldsDeclarer;
import org.joda.time.DateTime;
import org.ohmage.lifestreams.tuples.RecordTuple;
import org.ohmage.models.IUser;
import org.ohmage.models.Ohmage20User;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

abstract public class Ohmage20Spout<T> extends BaseLifestreamsSpout<T> {

    /**
     * the following fields are initialized in open() method **
     */

    // the ohmage user with which we will use to query the data
    final private Ohmage20User requester;

    public Ohmage20User getRequester() {
        return requester;
    }

    @Override
    protected List<IUser> getUsers() {
        List<IUser> users = new ArrayList<IUser>();
        for (String username : requester.getAccessibleUsers()) {
            users.add(new Ohmage20User(requester.getServer(), username, ""));
        }
        return users;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(RecordTuple.getFields());

    }

    public Ohmage20Spout(Ohmage20User requester, DateTime since, int retryDelay, TimeUnit unit) {
        super(since, retryDelay, unit);
        this.requester = requester;
    }

}
