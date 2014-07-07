package org.ohmage.lifestreams.oauth.client.providers;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.oltu.oauth2.client.OAuthClient;
import org.apache.oltu.oauth2.client.URLConnectionClient;
import org.apache.oltu.oauth2.client.request.OAuthClientRequest;
import org.apache.oltu.oauth2.client.response.OAuthJSONAccessTokenResponse;
import org.apache.oltu.oauth2.common.exception.OAuthProblemException;
import org.apache.oltu.oauth2.common.exception.OAuthSystemException;
import org.apache.oltu.oauth2.common.message.types.GrantType;
import org.apache.oltu.oauth2.common.token.BasicOAuthToken;
import org.apache.oltu.oauth2.common.token.OAuthToken;
import org.ohmage.models.Ohmage30Server;

import java.io.IOException;

/**
 * Created by changun on 6/28/14.
 */
public class OhmageOAuth extends OAuth20Provider {
    static public class OhmageOAuthToken extends BasicOAuthToken {

        public String getUserId() {
            return userId;
        }
        final private String userId;
        private  OhmageOAuthToken(OAuthToken token, String userId){
            super(token.getAccessToken(), token.getExpiresIn(), token.getRefreshToken(),  token.getScope());
            this.userId = userId;
        }
    }
    public OhmageOAuth(Ohmage30Server server, String apiKey, String apiSecret) {
        super("ohmage",
                server.getOAuthAuthURL(),
                "ohmage://app/oauth/authorize",
                server.getOAuthTokenURL(),
                apiKey, apiSecret);
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
        String clientId;
        try {
            clientId = new ObjectMapper().readTree(oAuthResponse.getBody()).get("user_id").asText();
        } catch (IOException e) {
            throw new OAuthSystemException("cannot locate userid");
        }

        return new OhmageOAuthToken(oAuthResponse.getOAuthToken(), clientId);
    }
}
