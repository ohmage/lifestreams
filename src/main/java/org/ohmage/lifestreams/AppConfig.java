package org.ohmage.lifestreams;

import org.joda.time.DateTime;
import org.ohmage.lifestreams.models.data.MobilityData;
import org.ohmage.lifestreams.spouts.MovesSpout;
import org.ohmage.lifestreams.spouts.OhmageStreamSpout;
import org.ohmage.lifestreams.stores.IMapStore;
import org.ohmage.lifestreams.stores.IStreamStore;
import org.ohmage.lifestreams.stores.RedisMapStore;
import org.ohmage.lifestreams.stores.RedisStreamStore;
import org.ohmage.models.OhmageStream;
import org.ohmage.models.OhmageUser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;

@Configuration
@PropertySource("/application.properties")
class AppConfig {

    private static final String LIFESTREAMS_OBSERVER_VER = "201403111";
    private static final String LIFESTREAMS_OBSERVER_ID = "org.ohmage.lifestreams.activities";

    @Autowired
    private
    Environment env;

    @Bean
    DateTime since() {
        return new DateTime(env.getProperty("since"));
    }

    @Bean
    OhmageUser requester() {
        return new OhmageUser(env.getProperty("ohmage.server"),
                env.getProperty("lifestreams.username"),
                env.getProperty("lifestreams.password"));
    }

    @Bean
    String requestees() {
        return env.getProperty("ohmage.requestees");
    }

    @Bean
    OhmageStream movesCredentialStream() {
        return new OhmageStream("org.ohmage.Moves", "oauth", "20140213",
                "20140213");
    }

    @Bean
    OhmageStream mobilityStream() {
        return new OhmageStream("edu.ucla.cens.Mobility", "extended",
                "2012061300", "2012050700");
    }

    @Bean
    OhmageStream activitySummaryStream() {
        return new OhmageStream(LIFESTREAMS_OBSERVER_ID, "activity_summaries",
                LIFESTREAMS_OBSERVER_VER, "20140311");
    }

    @Bean
    OhmageStream geodiameterStream() {
        return new OhmageStream(LIFESTREAMS_OBSERVER_ID, "geodiameter",
                LIFESTREAMS_OBSERVER_VER, "20140311");
    }

    @Bean
    OhmageStream leaveArriveHomeStream() {
        return new OhmageStream(LIFESTREAMS_OBSERVER_ID,
                "leave_arrive_home_time", LIFESTREAMS_OBSERVER_VER, "20140311");
    }

    @Bean
    MovesSpout movesSpout() {
        return new MovesSpout(since(), movesCredentialStream(),
                env.getProperty("com.moves.api.key"),
                env.getProperty("com.moves.api.secret"));
    }

    @Bean
    OhmageStreamSpout<MobilityData> mobilitySpout() {
        return new OhmageStreamSpout<MobilityData>(since(), MobilityData.class,
                mobilityStream(), "mode");
    }

    @Bean
    MobilityMovesTopology mobilityMovesTopology() {
        return new MobilityMovesTopology(env.getProperty("topology.name"),
                Integer.parseInt(env.getProperty("mobility.spout.number")),
                Integer.parseInt(env
                        .getProperty("parallelism.per.mobility.task")));
    }

    @Bean
    IMapStore mapStore() {
        return new RedisMapStore(env.getProperty("redis.host"));
    }

    @Bean
    IStreamStore streamStore() {
        return new RedisStreamStore(env.getProperty("redis.host"));
    }

    @Bean
    LifestreamsTopologyBuilder builder() {
        return new LifestreamsTopologyBuilder()
                .setRequester(requester())
                .setRequestees(requestees())
                .setColdStart(
                        Boolean.parseBoolean(env.getProperty("lifestreams.cold.start")))
                .setDryRun(
                        Boolean.parseBoolean(env.getProperty("lifestreams.dryrun")))
                .setMaxSpoutPending(
                        Integer.parseInt(env.getProperty("topology.max.spout.pending")))
                .setMsgTimeout(
                        Integer.parseInt(env.getProperty("topology.message.timeout.secs")))
                .setMapStore(mapStore())
                .setStreamStore(streamStore());

    }
}
