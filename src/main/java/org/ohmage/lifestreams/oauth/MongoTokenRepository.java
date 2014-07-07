package org.ohmage.lifestreams.oauth;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.*;
import org.apache.oltu.oauth2.common.token.BasicOAuthToken;
import org.apache.oltu.oauth2.common.token.OAuthToken;
import org.ohmage.models.Ohmage30User;

import java.io.Serializable;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A mongodb-based implementation of Token repository
 * Created by changun on 6/27/14.
 */
public class MongoTokenRepository implements TokenRepository<Ohmage30User>, Serializable {
    static private String host = "localhost";
    static  private ObjectMapper mapper = new ObjectMapper();
    private String getScopeKey(Scope scope) {
        // replace dots with underlines so that it works well with mongodb
        return (scope.getProvider() + ":" + scope.getScopeName()).replace(".", "_");
    }

    private String getScopeKeyId(Scope scope) {
        return "tokens." + this.getScopeKey(scope);
    }

    @Override
    public Set<Ohmage30User> getEntitiesWithScopes(Set<Scope> scopes) {

        DBCollection coll = this.getCollection();
        DBObject query = new BasicDBObject();

        for (Scope s : scopes) {
            query.put(getScopeKeyId(s),
                    new BasicDBObject("$exists", true));
        }
        DBCursor result = coll.find(query);
        Set<Ohmage30User> users = new HashSet<Ohmage30User>();
        for (DBObject obj : result) {
            users.add(mapper.convertValue(obj.get("entity"), Ohmage30User.class));
        }
        return users;
    }

    @Override
    public OAuthToken getToken(Ohmage30User entity, Scope scope) {
        DBCollection coll = this.getCollection();
        DBObject query = new BasicDBObject();
        query.put("_id", entity.getId());
        query.put("tokens." + scope.getProvider() + ":" + scope.getScopeName(),
                new BasicDBObject("$exists", true));
        DBObject result = coll.findOne(query);
        if (result != null) {
            Object obj = ((Map<Object, Object>) result.get("tokens")).get(getScopeKey(scope));
            return mapper.convertValue(obj, BasicOAuthToken.class);
        }
        return null;
    }

    @Override
    public void insertToken(Ohmage30User entity, Scope scope, OAuthToken token) {
        DBCollection coll = this.getCollection();
        DBObject query = new BasicDBObject();
        query.put("_id", entity.getId());
        DBObject obj = coll.findOne(query);
        if (obj == null) {
            obj = new BasicDBObject();
            obj.put("_id", entity.getId());
            obj.put("entity", mapper.convertValue(entity, BasicDBObject.class));
            obj.put("tokens", new HashMap<String, Object>());
        }
        BasicDBObject tokenObj = mapper.convertValue
                (token, BasicDBObject.class);
        ((Map<String, Object>) obj.get("tokens")).put(getScopeKey(scope), tokenObj);
        coll.save(obj);
    }


    @Override
    public void invalidateToken(Ohmage30User entity, Scope scope) {
        DBCollection coll = this.getCollection();
        DBObject query = new BasicDBObject();
        query.put("_id", entity.getId());
        DBObject obj = coll.findOne(query);
        if (obj != null) {
            ((Map<String, Object>) obj.get("tokens")).remove(getScopeKey(scope));
            coll.save(obj);
        }
    }


    static private class Singleton {
        final static private MongoClient client;

        static {
            try {
                client = new MongoClient(host);
            } catch (UnknownHostException e) {
                throw new RuntimeException(e);
            }
        }

    }

    public MongoTokenRepository(String host) {
        MongoTokenRepository.host =host;
    }

    public MongoClient getClient() {
        return Singleton.client;
    }

    public DBCollection getCollection() {
        DB db = getClient().getDB("token_repo");
        return db.getCollection("token");

    }

}
