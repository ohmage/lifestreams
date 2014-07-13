package org.ohmage.lifestreams;

import org.ohmage.lifestreams.models.data.MobilityData;
import org.ohmage.lifestreams.spouts.Ohmage20MovesSpout;
import org.ohmage.lifestreams.spouts.Ohmage20StreamSpout;
import org.ohmage.models.IStream;
import org.ohmage.models.Ohmage20Stream;
import org.ohmage.models.Ohmage20User;
import org.springframework.context.annotation.Bean;


public class Ohmage20AppConfig extends BaseAppConfig {

    private static final String LIFESTREAMS_OBSERVER_VER = "1";
    private static final String MOBILITY_OBSERVER_ID = "org.ohmage.lifestreams.mobility";
    private static final String MOVES_OBSERVER_ID = "org.ohmage.lifestreams.moves";

    @Bean
    Ohmage20User requester() {
        return new Ohmage20User(env.getProperty("ohmage.server"),
                env.getProperty("lifestreams.requester.username"),
                env.getProperty("lifestreams.requester.password"));
    }

    @Bean
    String requestees() {
        return env.getProperty("lifestreams.requestees");
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
    IStream movesSegmentStream() {
        return new Ohmage20Stream(MOVES_OBSERVER_ID,
                "moves_segment", LIFESTREAMS_OBSERVER_VER, "1");
    }
    @Bean
    Ohmage20MovesSpout movesSpout() {
        return new Ohmage20MovesSpout(requester(), requestees(), since(), movesCredentialStream(),
                env.getProperty("com.moves.api.key"),
                env.getProperty("com.moves.api.secret"));
    }

    @Bean
    Ohmage20StreamSpout<MobilityData> mobilitySpout() {
        return new Ohmage20StreamSpout<MobilityData>(requester(), requestees(), since(), MobilityData.class,
                mobilityStream(), "mode");
    }



}
