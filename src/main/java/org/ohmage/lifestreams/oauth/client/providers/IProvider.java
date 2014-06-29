package org.ohmage.lifestreams.oauth.client.providers;

import org.apache.oltu.oauth2.client.request.OAuthClientRequest;
import org.apache.oltu.oauth2.common.exception.OAuthProblemException;
import org.apache.oltu.oauth2.common.exception.OAuthSystemException;
import org.apache.oltu.oauth2.common.token.OAuthToken;

import java.io.Serializable;

public interface IProvider extends Serializable {

    public String getName();

    public OAuthClientRequest getAuthRequest(String callback,
                                             String state, String[] scope) throws OAuthSystemException;

    public OAuthToken getAccessToken(String code, String callback)
            throws OAuthSystemException, OAuthProblemException;

    public OAuthToken refreshToken(OAuthToken token)
            throws OAuthSystemException, OAuthProblemException;

}