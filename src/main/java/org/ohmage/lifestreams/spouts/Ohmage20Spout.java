package org.ohmage.lifestreams.spouts;

import org.joda.time.DateTime;
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
    final private String requestees;

    public Ohmage20User getRequester() {
        return requester;
    }

    @Override
    protected List<IUser> getUsers() {
        List<IUser> users = new ArrayList<IUser>();
        if(this.requestees == null || this.requestees.length()==0) {
            for (String username : requester.getAccessibleUsers()) {
                users.add(new Ohmage20User(requester.getServer(), username, ""));
            }
        }else{
            for (String username : this.requestees.split(",")) {
                users.add(new Ohmage20User(requester.getServer(), username, ""));
            }
        }
        return users;
    }


    public Ohmage20Spout(Ohmage20User requester, DateTime since, int retryDelay, TimeUnit unit) {
        this(requester, "", since, retryDelay, unit);
    }
    public Ohmage20Spout(Ohmage20User requester, String requestees, DateTime since, int retryDelay, TimeUnit unit) {
        super(since, retryDelay, unit);
        this.requester = requester;
        this.requestees = requestees;
    }

}
