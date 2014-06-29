package org.ohmage.lifestreams.oauth.client.providers;

import org.apache.commons.lang3.StringUtils;
import org.apache.oltu.oauth2.client.OAuthClient;
import org.apache.oltu.oauth2.client.URLConnectionClient;
import org.apache.oltu.oauth2.client.request.OAuthClientRequest;
import org.apache.oltu.oauth2.client.response.OAuthJSONAccessTokenResponse;
import org.apache.oltu.oauth2.common.exception.OAuthProblemException;
import org.apache.oltu.oauth2.common.exception.OAuthSystemException;
import org.apache.oltu.oauth2.common.message.types.GrantType;
import org.apache.oltu.oauth2.common.token.BasicOAuthToken;
import org.apache.oltu.oauth2.common.token.OAuthToken;


public abstract class OAuth20Provider implements IProvider {


    final private String apiKey;
    final private String apiSecret;
    final private String authEndpoint;
    final private String accessTokenEndpoint;
    final private String name;

    String getApiKey() {
        return apiKey;
    }

    String getApiSecret() {
        return apiSecret;
    }

    String getAuthEndpoint() {
        return authEndpoint;
    }

    String getAccessTokenEndpoint() {
        return accessTokenEndpoint;
    }


    public String getName() {
        return name;
    }

    @Override
    public OAuthClientRequest getAuthRequest(String callback, String state,
                                             String[] scopes) throws OAuthSystemException {
        return OAuthClientRequest
                .authorizationLocation(getAuthEndpoint())
                .setClientId(getApiKey())
                .setRedirectURI(callback)
                .setState(state)
                .setScope(StringUtils.join(scopes, ','))
                .setResponseType("code")
                .buildQueryMessage();
    }

    @Override
    public OAuthToken getAccessToken(String code, String callback) throws OAuthSystemException, OAuthProblemException {
        OAuthClientRequest request = OAuthClientRequest
                .tokenLocation(getAccessTokenEndpoint())
                .setGrantType(GrantType.AUTHORIZATION_CODE)
                .setClientId(getApiKey())
                .setClientSecret(getApiSecret())
                .setRedirectURI(callback)
                .setCode(code)
                .buildBodyMessage();

        OAuthClient oAuthClient = new OAuthClient(new URLConnectionClient());
        OAuthJSONAccessTokenResponse oAuthResponse = oAuthClient.accessToken(request);
        return oAuthResponse.getOAuthToken();
    }

    @Override
    public OAuthToken refreshToken(OAuthToken token) throws OAuthSystemException, OAuthProblemException {
        OAuthClientRequest request = OAuthClientRequest
                .tokenLocation(getAccessTokenEndpoint())
                .setGrantType(GrantType.REFRESH_TOKEN)
                .setClientId(getApiKey())
                .setClientSecret(getApiSecret())
                .setRefreshToken(token.getRefreshToken())
                .buildBodyMessage();
        OAuthClient oAuthClient = new OAuthClient(new URLConnectionClient());
        OAuthJSONAccessTokenResponse oAuthResponse = oAuthClient.accessToken(request);
        OAuthToken newToken = oAuthResponse.getOAuthToken();
        if (newToken.getRefreshToken() == null || newToken.getRefreshToken().length() == 0) {
            // the provider does not return a new refresh token, keep using the original refresh token
            newToken = new BasicOAuthToken(newToken.getAccessToken(), newToken.getExpiresIn(), token.getRefreshToken(), newToken.getScope());
        }
        return newToken;

    }

    OAuth20Provider(String name, String authEndpoint, String accessTokenEndpoint,
                    String apiKey, String apiSecret) {
        this.name = name;
        this.authEndpoint = authEndpoint;
        this.accessTokenEndpoint = accessTokenEndpoint;
        this.apiKey = apiKey;
        this.apiSecret = apiSecret;
    }


}
