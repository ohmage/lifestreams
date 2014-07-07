package org.ohmage.lifestreams;

import co.nutrino.api.moves.impl.dto.storyline.MovesSegment;
import org.joda.time.DateTime;
import org.ohmage.lifestreams.models.data.MobilityData;
import org.ohmage.lifestreams.oauth.MongoTokenRepository;
import org.ohmage.lifestreams.oauth.TokenManager;
import org.ohmage.lifestreams.spouts.BaseLifestreamsSpout;
import org.ohmage.lifestreams.spouts.Ohmage20MovesSpout;
import org.ohmage.lifestreams.stores.IMapStore;
import org.ohmage.lifestreams.stores.IStreamStore;
import org.ohmage.lifestreams.stores.MongoStreamStore;
import org.ohmage.lifestreams.stores.RedisMapStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;

/**
 * Created by changun on 6/29/14.
 */
@Configuration
@PropertySource("/lifestreams.properties")
abstract public class BaseAppConfig {
    @Autowired
    protected  Environment env;

    @Bean
    DateTime since() {
        return new DateTime(env.getProperty("since"));
    }

    @Bean
    public IMapStore mapStore() {
        return new RedisMapStore(env.getProperty("redis.host"));
    }

    @Bean
    public IStreamStore streamStore() {
        return new MongoStreamStore(env.getProperty("mongo.host"));
    }

    @Bean
    abstract BaseLifestreamsSpout<MobilityData> mobilitySpout();

    @Bean
    abstract BaseLifestreamsSpout<MovesSegment> movesSpout();

    @Bean
    MobilityMovesTopology mobilityMovesTopology() {
        return new MobilityMovesTopology(env.getProperty("topology.name"),
                Integer.parseInt(env.getProperty("mobility.spout.number")),
                Integer.parseInt(env
                        .getProperty("parallelism.per.mobility.task")));
    }

    @Bean
    LifestreamsTopologyBuilder builder() {
        return new LifestreamsTopologyBuilder()
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
