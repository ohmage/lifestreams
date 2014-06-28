package org.ohmage.lifestreams.models;

/**
 * Created by changun on 6/27/14.
 */
public class Ohmage30User {
    String userId;


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Ohmage30User that = (Ohmage30User) o;

        if (userId != null ? !userId.equals(that.userId) : that.userId != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return userId != null ? userId.hashCode() : 0;
    }

    public String getUserId() {

        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }


    public Ohmage30User(String userId) {
        this.userId = userId;
    }

    public Ohmage30User() {}
}
