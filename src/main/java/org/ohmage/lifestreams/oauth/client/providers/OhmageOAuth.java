package org.ohmage.lifestreams.oauth.client.providers;

import org.ohmage.models.Ohmage30Server;

/**
 * Created by changun on 6/28/14.
 */
public class OhmageOAuth extends OAuth20Provider {
    public OhmageOAuth(Ohmage30Server server, String apiKey, String apiSecret) {
        super("ohmage",
                server.getOAuthAuthURL(),
                server.getOAuthTokenURL(),
                apiKey, apiSecret);
    }
}
