package org.ohmage.lifestreams;

import org.joda.time.DateTime;
import org.ohmage.lifestreams.oauth.client.providers.GoogleProfileProvider;
import org.ohmage.lifestreams.oauth.client.providers.IProvider;
import org.ohmage.lifestreams.spouts.GmailSpout;
import org.ohmage.lifestreams.tasks.MailCleaner;
import org.ohmage.models.OhmageStream;
import org.ohmage.models.OhmageUser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
class EmailTopology {

    private IProvider gmailProvider = new GoogleProfileProvider("gmail", "",
            "48636836762-9p082qvhat6ojtgnhn4najkmkuolaieu.apps.googleusercontent.com",
            "_flzl2k8JySJKnXsHxkQjqFv",
            "https://mail.google.com/ https://www.googleapis.com/auth/userinfo.email");

    private OhmageStream accessTokenStream = new OhmageStream("org.ohmage.oauth", "token", "201405312", "201405312");

    @Autowired
    private
    OhmageUser requester;
    @Value("${ohmage.requestees}")
    private
    String requestees;

    public void run() {
        LifestreamsTopologyBuilder builder = new LifestreamsTopologyBuilder();
        builder
                .setRequester(requester)
                .setRequestees(requestees)
                .setColdStart(true);

        GmailSpout spout = new GmailSpout(gmailProvider, accessTokenStream, new DateTime().minusMonths(1));
        builder.setSpout("GmailSpout", spout);
        builder.setTask("StripEmail", new MailCleaner(), "GmailSpout");
        builder.submitToLocalCluster("EmailTest");
    }
}
