package org.ohmage.lifestreams.oauth;

import org.apache.oltu.oauth2.common.exception.OAuthProblemException;
import org.apache.oltu.oauth2.common.exception.OAuthSystemException;
import org.apache.oltu.oauth2.common.token.OAuthToken;
import org.ohmage.lifestreams.oauth.client.providers.IProvider;
import org.ohmage.models.Ohmage30User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

/**
 * Created by changun on 6/27/14.
 */
public class TokenManager implements Serializable{
    final private static Logger logger = LoggerFactory.getLogger(TokenManager.class);
    TokenRepository<Ohmage30User> repo;
    Map<String, IProvider> providers;

    public Set<Ohmage30User> getUsersWithScopes(Set<Scope> scopes) {
        return repo.getEntitiesWithScopes(scopes);
    }

    public OAuthToken getToken(Ohmage30User user, Scope scope) {
        return repo.getToken(user, scope);
    }

    public void insertToken(Ohmage30User user, Scope scope, OAuthToken token) {
        repo.insertToken(user, scope, token);
    }

    public OAuthToken refreshToken(Ohmage30User user, Scope scope) {
        OAuthToken token = repo.getToken(user, scope);
        if (token != null) {
            IProvider provider = providers.get(scope.getProvider());
            try {
                OAuthToken newToken = provider.refreshToken(token);
                for (String scopeName : newToken.getScope().split(" ")) {
                    Scope s = new Scope(provider.getName(), scopeName);
                    this.insertToken(user, s, newToken);
                }
                return newToken;
            } catch (OAuthSystemException e) {
                logger.error("Fail to refresh token", e);
            } catch (OAuthProblemException e) {
                repo.invalidateToken(user, scope);
                logger.error("Refresh token is invalid! Invalidate the token", e);
            }

        }
        return null;
    }

    public TokenManager(TokenRepository<Ohmage30User> repo, Map<String, IProvider> providers) {
        this.repo = repo;
        this.providers = providers;
    }
}
