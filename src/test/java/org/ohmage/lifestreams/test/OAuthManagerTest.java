package org.ohmage.lifestreams.test;

import org.apache.oltu.oauth2.common.token.BasicOAuthToken;
import org.junit.Assert;
import org.junit.Test;
import org.ohmage.lifestreams.oauth.MongoTokenRepository;
import org.ohmage.lifestreams.oauth.Scope;
import org.ohmage.lifestreams.oauth.TokenManager;
import org.ohmage.lifestreams.oauth.client.providers.GoogleOAuth;
import org.ohmage.lifestreams.oauth.client.providers.IProvider;
import org.ohmage.models.Ohmage30User;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by changun on 6/28/14.
 */
public class OAuthManagerTest {
    Ohmage30User testUser = new Ohmage30User("THISISATESTUSER");

    IProvider provider = new GoogleOAuth("", "");
    MongoTokenRepository repo = new MongoTokenRepository("localhost");
    BasicOAuthToken token = new BasicOAuthToken("TESTTOKEN");
    Scope scope = new Scope(provider.getName(), "TESTSCOPE1");
    Scope scope2 = new Scope(provider.getName(), "TESTSCOPE2");
    Set<Scope> scopeSet;

    {
        scopeSet = new HashSet<Scope>();
        scopeSet.add(scope);
        scopeSet.add(scope2);
    }

    @Test
    public void testInsertGetAndRemoveToken() {
        Map<String, IProvider> providerMap = new HashMap<String, IProvider>();
        providerMap.put(provider.getName(), provider);
        TokenManager mng = new TokenManager(repo, providerMap);
        // both of the refreshToken operations should fail and return null
        Assert.assertNull(mng.refreshToken(testUser, scope));
        Assert.assertNull(mng.refreshToken(testUser, scope2));

        // we shouldn't find any valid user at this point
        Assert.assertFalse(mng.getUsersWithScopes(scopeSet).contains(testUser));

        // insert fake tokens
        mng.insertToken(testUser, scope, token);
        mng.insertToken(testUser, scope2, token);

        // we should be able to find the test user
        Assert.assertTrue(mng.getUsersWithScopes(scopeSet).contains(testUser));
        // check the content of the toekn
        Assert.assertEquals(mng.getToken(testUser, scope).getAccessToken(), "TESTTOKEN");
        // refresh token should fail again..
        Assert.assertNull(mng.refreshToken(testUser, scope));
    }

}
