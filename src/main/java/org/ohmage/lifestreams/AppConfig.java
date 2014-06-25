package org.ohmage.lifestreams;

import org.joda.time.DateTime;
import org.ohmage.lifestreams.models.data.MobilityData;
import org.ohmage.lifestreams.spouts.MovesSpout;
import org.ohmage.lifestreams.spouts.OhmageStreamSpout;
import org.ohmage.lifestreams.stores.*;
import org.ohmage.models.OhmageStream;
import org.ohmage.models.OhmageUser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;
import redis.clients.jedis.JedisPoolConfig;

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
        return new OhmageStream(MOBILITY_OBSERVER_ID, "summaries",
                LIFESTREAMS_OBSERVER_VER, "1");
    }

    @Bean
    OhmageStream geodiameterStream() {
        return new OhmageStream(MOBILITY_OBSERVER_ID, "geodiameter",
                LIFESTREAMS_OBSERVER_VER, "1");
    }

    @Bean
    OhmageStream leaveReturnHomeStream() {
        return new OhmageStream(MOBILITY_OBSERVER_ID,
                "leave_return_home_time", LIFESTREAMS_OBSERVER_VER, "1");
    }
    @Bean
    OhmageStream dataCoverageStream() {
        return new OhmageStream(MOBILITY_OBSERVER_ID,
                "coverage", LIFESTREAMS_OBSERVER_VER, "1");
    }

    @Bean
    OhmageStream activitySummaryStreamForMoves() {
        return new OhmageStream(MOVES_OBSERVER_ID, "summaries",
                LIFESTREAMS_OBSERVER_VER, "1");
    }

    @Bean
    OhmageStream geodiameterStreamForMoves() {
        return new OhmageStream(MOVES_OBSERVER_ID, "geodiameter",
                LIFESTREAMS_OBSERVER_VER, "1");
    }

    @Bean
    OhmageStream leaveReturnHomeStreamForMoves() {
        return new OhmageStream(MOVES_OBSERVER_ID,
                "leave_return_home_time", LIFESTREAMS_OBSERVER_VER, "1");
    }

    @Bean
    OhmageStream dataCoverageStreamForMoves() {
        return new OhmageStream(MOVES_OBSERVER_ID,
                "coverage", LIFESTREAMS_OBSERVER_VER, "1");
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
