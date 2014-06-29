package org.ohmage.lifestreams.oauth.client.providers;

@SuppressWarnings("SameParameterValue")
public class GoogleOAuth extends OAuth20Provider {

    public GoogleOAuth(
            String apiKey, String apiSecret) {
        super("google",
                "https://accounts.google.com/o/oauth2/auth?approval_prompt=force&access_type=offline", // force approval so that we can get refresh_token every time!
                "https://accounts.google.com/o/oauth2/token",
                apiKey, apiSecret);

    }


}
