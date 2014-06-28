package org.ohmage.lifestreams.oauth.client.providers;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.oltu.oauth2.client.OAuthClient;
import org.apache.oltu.oauth2.client.URLConnectionClient;
import org.apache.oltu.oauth2.client.request.OAuthBearerClientRequest;
import org.apache.oltu.oauth2.client.request.OAuthClientRequest;
import org.apache.oltu.oauth2.client.response.OAuthResourceResponse;
import org.apache.oltu.oauth2.common.OAuth;
import org.apache.oltu.oauth2.common.token.OAuthToken;

@SuppressWarnings("SameParameterValue")
public class GoogleOAuth extends OAuth20Provider {

	public GoogleOAuth(
            String apiKey, String apiSecret) {
		super("google",
			  "https://accounts.google.com/o/oauth2/auth?approval_prompt=force&access_type=offline", // force approval so that we can get refresh_token every time!
			  "https://accounts.google.com/o/oauth2/token",
			  apiKey,	apiSecret);

	}



}
