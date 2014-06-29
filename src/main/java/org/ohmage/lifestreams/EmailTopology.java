package org.ohmage.lifestreams;

import org.joda.time.DateTime;
import org.ohmage.lifestreams.oauth.client.providers.GoogleOAuth;
import org.ohmage.lifestreams.oauth.client.providers.IProvider;
import org.ohmage.lifestreams.spouts.Ohmage20GmailSpout;
import org.ohmage.lifestreams.tasks.MailCleaner;
import org.ohmage.models.Ohmage20Stream;
import org.ohmage.models.Ohmage20User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
class EmailTopology {

    private IProvider gmailProvider = new GoogleOAuth(
            "48636836762-9p082qvhat6ojtgnhn4najkmkuolaieu.apps.googleusercontent.com",
            "_flzl2k8JySJKnXsHxkQjqFv");

    private Ohmage20Stream accessTokenStream = new Ohmage20Stream("org.ohmage.oauth", "token", "201405312", "201405312");

    @Autowired
    private
    Ohmage20User requester;
    @Value("${ohmage.requestees}")
    private
    String requestees;

    public void run() {
        LifestreamsTopologyBuilder builder = new LifestreamsTopologyBuilder();
        builder
                .setRequester(requester)
                .setRequestees(requestees)
                .setColdStart(true);

        Ohmage20GmailSpout spout = new Ohmage20GmailSpout(gmailProvider, accessTokenStream, new DateTime().minusMonths(1));
        builder.setSpout("GmailSpout", spout);
        builder.setTask("StripEmail", new MailCleaner(), "GmailSpout");
        builder.submitToLocalCluster("EmailTest");
    }
}
