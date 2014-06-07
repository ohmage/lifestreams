package org.ohmage.lifestreams.oauth.client.providers;

import org.apache.oltu.oauth2.client.OAuthClient;
import org.apache.oltu.oauth2.client.URLConnectionClient;
import org.apache.oltu.oauth2.client.request.OAuthClientRequest;
import org.apache.oltu.oauth2.client.response.OAuthJSONAccessTokenResponse;
import org.apache.oltu.oauth2.common.exception.OAuthProblemException;
import org.apache.oltu.oauth2.common.exception.OAuthSystemException;
import org.apache.oltu.oauth2.common.message.types.GrantType;
import org.apache.oltu.oauth2.common.token.BasicOAuthToken;
import org.apache.oltu.oauth2.common.token.OAuthToken;


public class OAuth20Provider implements IProvider {

	final private String apiName;
	final private String apiKey;
	final private String apiSecret;
	final private String scope;
	final private String name;
	final private String authEndpoint;
	final private String accessToeknEndpoint;

	
	/* (non-Javadoc)
	 * @see org.ohmage.oauth_client.providers.IProvider#getApiName()
	 */
	@Override
	public String getApiName() {
		return apiName;
	}
	public String getApiKey() {
		return apiKey;
	}
	public String getApiSecret() {
		return apiSecret;
	}
	/* (non-Javadoc)
	 * @see org.ohmage.oauth_client.providers.IProvider#getScope()
	 */
	@Override
	public String getScope() {
		return scope;
	}
	/* (non-Javadoc)
	 * @see org.ohmage.oauth_client.providers.IProvider#getName()
	 */
	@Override
	public String getName() {
		return name;
	}
	public String getAuthEndpoint() {
		return authEndpoint;
	}
	public String getAccessToeknEndpoint() {
		return accessToeknEndpoint;
	}

	/* (non-Javadoc)
	 * @see org.ohmage.oauth_client.providers.IProvider#getAuthRequest(java.lang.String, java.lang.String)
	 */
	@Override
	public OAuthClientRequest getAuthRequest(String callback, String state) throws OAuthSystemException{
		return OAuthClientRequest
				   .authorizationLocation(getAuthEndpoint())
				   .setClientId(getApiKey())
				   .setRedirectURI(callback)
				   .setState(state)
				   .setScope(getScope())
				   .setResponseType("code")
				   .buildQueryMessage();
	}
	/* (non-Javadoc)
	 * @see org.ohmage.oauth_client.providers.IProvider#getAccessToken(java.lang.String, java.lang.String)
	 */
	@Override
	public OAuthToken getAccessToken(String code, String callback) throws OAuthSystemException, OAuthProblemException{
		OAuthClientRequest request = OAuthClientRequest
                .tokenLocation(getAccessToeknEndpoint())
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
	/* (non-Javadoc)
	 * @see org.ohmage.oauth_client.providers.IProvider#getMetaInfo(org.apache.oltu.oauth2.common.token.OAuthToken)
	 */
	@Override
	public Object getMetaInfo(OAuthToken token){
		return null;
	}
	/* (non-Javadoc)
	 * @see org.ohmage.oauth_client.providers.IProvider#refreshToken(org.apache.oltu.oauth2.common.token.OAuthToken)
	 */
	@Override
	public OAuthToken refreshToken(OAuthToken token) throws OAuthSystemException, OAuthProblemException{
		OAuthClientRequest request = OAuthClientRequest
                .tokenLocation(getAccessToeknEndpoint())
                .setGrantType(GrantType.REFRESH_TOKEN)
                .setClientId(getApiKey())
                .setClientSecret(getApiSecret())
                .setRefreshToken(token.getRefreshToken())
                .buildBodyMessage();
    	OAuthClient oAuthClient = new OAuthClient(new URLConnectionClient());
    	OAuthJSONAccessTokenResponse oAuthResponse = oAuthClient.accessToken(request);
    	OAuthToken newToken = oAuthResponse.getOAuthToken();
    	if(newToken.getRefreshToken() == null || newToken.getRefreshToken().length() == 0){
    		// the provider does not return a new refresh token, keep using the original refresh token
    		newToken = new BasicOAuthToken(newToken.getAccessToken(), newToken.getExpiresIn(),token.getRefreshToken(),  newToken.getScope());
    	}
    	return newToken;
    	
	}
	public OAuth20Provider(String providerName, 
			String authEndpoint, String accessTokenEndpoint, 
			String apiName, String apiKey, String apiSecret, 
			String scope) {
		
		this.name = providerName;
		this.authEndpoint = authEndpoint;
		this.accessToeknEndpoint = accessTokenEndpoint;
		this.apiName = apiName;
		this.apiKey = apiKey;
		this.apiSecret = apiSecret;
		this.scope = scope;
	}


}
