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
public class GoogleProfileProvider extends OAuth20Provider {

	public GoogleProfileProvider(String providerName,
			String apiName,	String apiKey, String apiSecret, String scope) {
		super(providerName, 
			  "https://accounts.google.com/o/oauth2/auth?approval_prompt=force&access_type=offline", // force approval so that we can get refresh_token every time! 
			  "https://accounts.google.com/o/oauth2/token", 
			  apiName, apiKey,	apiSecret, scope);
		if(!scope.contains("https://www.googleapis.com/auth/userinfo")){
			throw new RuntimeException("Must contain the user profile scope: https://www.googleapis.com/auth/userinfo.profile");
		}
	}

	@Override
	public Object getMetaInfo(OAuthToken token){
		try{
			OAuthClientRequest req = 
					new OAuthBearerClientRequest("https://www.googleapis.com/oauth2/v1/userinfo?alt=json")
	        		   .setAccessToken(token.getAccessToken())
	        		   .buildQueryMessage();
	    	OAuthClient oAuthClient = new OAuthClient(new URLConnectionClient());
	    	OAuthResourceResponse resourceResponse = oAuthClient.resource(req, 
	    											OAuth.HttpMethod.GET, OAuthResourceResponse.class);
	    	ObjectMapper mapper = new ObjectMapper();
	    	return mapper.readTree((resourceResponse.getBody()));
		}catch(Exception e){
			throw new RuntimeException(e);
		}
	}

}
