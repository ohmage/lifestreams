package org.ohmage.lifestreams;

import org.joda.time.DateTime;
import org.ohmage.lifestreams.models.data.MobilityData;
import org.ohmage.lifestreams.spouts.Ohmage20MovesSpout;
import org.ohmage.lifestreams.spouts.Ohmage20StreamSpout;
import org.ohmage.lifestreams.stores.IMapStore;
import org.ohmage.lifestreams.stores.IStreamStore;
import org.ohmage.lifestreams.stores.MongoStreamStore;
import org.ohmage.lifestreams.stores.RedisMapStore;
import org.ohmage.models.IStream;
import org.ohmage.models.Ohmage20Stream;
import org.ohmage.models.Ohmage20User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;

@Configuration
@PropertySource("/lifestreams.properties")
public class AppConfig {

    private static final String LIFESTREAMS_OBSERVER_VER = "1";
    private static final String MOBILITY_OBSERVER_ID = "org.ohmage.lifestreams.mobility";
    private static final String MOVES_OBSERVER_ID = "org.ohmage.lifestreams.moves";

    @Autowired
    private
    Environment env;

    @Bean
    DateTime since() {
        return new DateTime(env.getProperty("since"));
    }

    @Bean
    Ohmage20User requester() {
        return new Ohmage20User(env.getProperty("ohmage.server"),
                env.getProperty("lifestreams.username"),
                env.getProperty("lifestreams.password"));
    }

    @Bean
    String requestees() {
        return env.getProperty("ohmage.requestees");
    }

    @Bean
    Ohmage20Stream movesCredentialStream() {
        return new Ohmage20Stream("org.ohmage.Moves", "oauth", "20140213",
                "20140213");
    }

    @Bean
    Ohmage20Stream mobilityStream() {
        return new Ohmage20Stream("edu.ucla.cens.Mobility", "extended",
                "2012061300", "2012050700");
    }

    @Bean
    IStream activitySummaryStream() {
        return new Ohmage20Stream(MOBILITY_OBSERVER_ID, "summaries",
                LIFESTREAMS_OBSERVER_VER, "1");
    }

    @Bean
    IStream geodiameterStream() {
        return new Ohmage20Stream(MOBILITY_OBSERVER_ID, "geodiameter",
                LIFESTREAMS_OBSERVER_VER, "1");
    }

    @Bean
    IStream leaveReturnHomeStream() {
        return new Ohmage20Stream(MOBILITY_OBSERVER_ID,
                "leave_return_home_time", LIFESTREAMS_OBSERVER_VER, "1");
    }

    @Bean
    IStream dataCoverageStream() {
        return new Ohmage20Stream(MOBILITY_OBSERVER_ID,
                "coverage", LIFESTREAMS_OBSERVER_VER, "1");
    }

    @Bean
    IStream activitySummaryStreamForMoves() {
        return new Ohmage20Stream(MOVES_OBSERVER_ID, "summaries",
                LIFESTREAMS_OBSERVER_VER, "1");
    }

    @Bean
    IStream geodiameterStreamForMoves() {
        return new Ohmage20Stream(MOVES_OBSERVER_ID, "geodiameter",
                LIFESTREAMS_OBSERVER_VER, "1");
    }

    @Bean
    IStream leaveReturnHomeStreamForMoves() {
        return new Ohmage20Stream(MOVES_OBSERVER_ID,
                "leave_return_home_time", LIFESTREAMS_OBSERVER_VER, "1");
    }

    @Bean
    IStream dataCoverageStreamForMoves() {
        return new Ohmage20Stream(MOVES_OBSERVER_ID,
                "coverage", LIFESTREAMS_OBSERVER_VER, "1");
    }

    @Bean
    Ohmage20MovesSpout movesSpout() {
        return new Ohmage20MovesSpout(since(), movesCredentialStream(),
                env.getProperty("com.moves.api.key"),
                env.getProperty("com.moves.api.secret"));
    }

    @Bean
    Ohmage20StreamSpout<MobilityData> mobilitySpout() {
        return new Ohmage20StreamSpout<MobilityData>(since(), MobilityData.class,
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
    public IMapStore mapStore() {
        return new RedisMapStore(env.getProperty("redis.host"));
    }

    @Bean
    public IStreamStore streamStore() {
        return new MongoStreamStore(env.getProperty("mongo.host"));
        /*
        return new RedisStreamStore(env.getProperty("redis.host"), new JedisPoolConfig(),
                Integer.parseInt(env.getProperty("redis.stream.store.DBIndex")));*/
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
