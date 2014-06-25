package org.ohmage.lifestreams.oauth.client.providers;

import org.apache.oltu.oauth2.client.request.OAuthClientRequest;
import org.apache.oltu.oauth2.common.exception.OAuthProblemException;
import org.apache.oltu.oauth2.common.exception.OAuthSystemException;
import org.apache.oltu.oauth2.common.token.OAuthToken;

import java.io.Serializable;

public interface IProvider extends Serializable{

	public abstract String getApiName();

	public abstract String getScope();

	public abstract String getName();

	public abstract OAuthClientRequest getAuthRequest(String callback,
			String state) throws OAuthSystemException;

	public abstract OAuthToken getAccessToken(String code, String callback)
			throws OAuthSystemException, OAuthProblemException;

	public abstract Object getMetaInfo(OAuthToken token);

	public abstract OAuthToken refreshToken(OAuthToken token)
			throws OAuthSystemException, OAuthProblemException;

}