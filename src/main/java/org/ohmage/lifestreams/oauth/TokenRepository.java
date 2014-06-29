package org.ohmage.lifestreams.oauth;

import org.apache.oltu.oauth2.common.token.OAuthToken;

import java.util.Set;

/**
 * Created by changun on 6/27/14.
 */
public interface TokenRepository<T> {
    Set<T> getEntitiesWithScopes(Set<Scope> scopes);

    OAuthToken getToken(T entity, Scope scope);

    void insertToken(T entity, Scope scope, OAuthToken token);

    void invalidateToken(T entity, Scope scope);
}
