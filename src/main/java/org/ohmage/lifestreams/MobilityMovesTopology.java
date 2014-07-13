package org.ohmage.lifestreams;

import co.nutrino.api.moves.impl.dto.storyline.MovesSegment;
import org.joda.time.Days;
import org.joda.time.Duration;
import org.joda.time.Hours;
import org.ohmage.lifestreams.models.data.MobilityData;
import org.ohmage.lifestreams.spouts.BaseLifestreamsSpout;
import org.ohmage.lifestreams.tasks.DataRateLimiter;
import org.ohmage.lifestreams.tasks.GeoDiameterTask;
import org.ohmage.lifestreams.tasks.mobility.*;
import org.ohmage.lifestreams.tasks.moves.*;
import org.ohmage.models.Ohmage20Stream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

/**
 * This topology processes Mobility and Moves data and generate daily
 * summaries including: daily activity summaries, daily geodiameter, and time
 * leave/return home.
 *
 * @author changun
 */
class MobilityMovesTopology {

    // ** Mobility Output streams **//
    @Autowired(required=false)
    @Qualifier("activitySummaryStream")
    private
    Ohmage20Stream activitySummaryStream;
    @Autowired(required=false)
    @Qualifier("geodiameterStream")
    private
    Ohmage20Stream geodiameterStream;
    @Autowired(required=false)
    @Qualifier("leaveReturnHomeStream")
    private
    Ohmage20Stream leaveReturnHomeStream;
    @Autowired(required=false)
    @Qualifier("dataCoverageStream")
    private
    Ohmage20Stream dataCoverageStream;

    // ** Moves Output streams **//
    @Autowired(required=false)
    @Qualifier("activitySummaryStreamForMoves")
    private
    Ohmage20Stream activitySummaryStreamForMoves;
    @Autowired(required=false)
    @Qualifier("geodiameterStreamForMoves")
    private
    Ohmage20Stream geodiameterStreamForMoves;
    @Autowired(required=false)
    @Qualifier("leaveReturnHomeStreamForMoves")
    private
    Ohmage20Stream leaveReturnHomeStreamForMoves;
    @Autowired(required=false)
    @Qualifier("dataCoverageStreamForMoves")
    private
    Ohmage20Stream dataCoverageStreamForMoves;
    @Autowired(required=false)
    @Qualifier("movesSegmentStream")
    private
    Ohmage20Stream movesSegmentStream;

    // ** Spouts ** //
    @Autowired
    @Qualifier("mobilitySpout")
    private
    BaseLifestreamsSpout<MobilityData> mobilitySpout;

    @Autowired
    @Qualifier("movesSpout")
    private
    BaseLifestreamsSpout<MovesSegment> movesSpout;
    @Autowired
    private
    LifestreamsTopologyBuilder builder;
    // ** Configuration ** //
    private final String topologyName;
    private final int parallelismPerTask;
    private final int mobility_spout_number;

    public void run() {
        /** setup the topology **/

        /**
         * Topology part 1. create a spout that gets mobility data and the tasks
         * that consume the data
         **/

        builder.setSpout("MobilityDataStream", mobilitySpout,
                mobility_spout_number);
        builder.setTask("ThrottledMobilityDataStream", new DataRateLimiter(Duration.standardSeconds(30)), "MobilityDataStream")
                .setParallelismHint(parallelismPerTask);
        builder.setTask("MobilityHourlyDataCoverage", new MobilityCoverage(Hours.ONE), "ThrottledMobilityDataStream")
                .setTargetStream(dataCoverageStream)
                .setParallelismHint(parallelismPerTask);
        builder.setTask("PlaceDetection", new PlaceDetection(), "ThrottledMobilityDataStream")
                .setParallelismHint(parallelismPerTask);

        builder.setTask("MobilityTimeLeaveReturnHome", new TimeLeaveReturnHome(), "PlaceDetection")
                .setParallelismHint(parallelismPerTask)
                .setTargetStream(leaveReturnHomeStream);

        // compute daily geodiameter from Mobility data
        builder.setTask("GeoDistanceBolt", new GeoDiameterTask(), "ThrottledMobilityDataStream")
                .setTargetStream(geodiameterStream)
                .setParallelismHint(parallelismPerTask)
                .setTimeWindowSize(Days.ONE);

        // HMM model to correct shor-term errors in Mobility
        builder.setTask("HMMMobilityStateRectifier", new HMMMobilityRectifier(), "ThrottledMobilityDataStream")
                .setParallelismHint(parallelismPerTask);
        // based on the corrected Mobility data, compute daily activity
        // summary
        builder.setTask("MobilityActivitySummarier",
                new MobilityActivitySummarizer(), "HMMMobilityStateRectifier")
                .setParallelismHint(parallelismPerTask)
                .setTargetStream(activitySummaryStream)
                .setTimeWindowSize(Days.ONE);


        /**
         * Topology part 2. create a spout that gets Moves data and the tasks
         * that consume the data
         **/
        if(movesSpout != null) {
            builder.setSpout("RawMovesDataStream", movesSpout, 1);

            // segments from the ohmage or the local Moves fetcher may contain
            // duplication. Filter them out.
            builder.setTask("MovesDataStream", new FilterDuplicatedSegment(), "RawMovesDataStream")
                    .setTargetStream(movesSegmentStream);

            //compute hourly coverage rate
            builder.setTask("MovesHourlyDataCoverage", new MovesDataCoverage(Hours.ONE), "MovesDataStream")
                    .setTargetStream(dataCoverageStreamForMoves);

            // extract track points from moves segments
            builder.setTask("MovesTrackPointExtractor", new TrackPointExtractor(),
                    "MovesDataStream");

            // compute geo diameter based on the track points
            builder.setTask("MovesGeoDiameter", new GeoDiameterTask(),
                    "MovesTrackPointExtractor")
                    .setTargetStream(geodiameterStreamForMoves)
                    .setTimeWindowSize(Days.ONE);

            // generate daily activity summary
            builder.setTask("MovesActivitySummarier", new MovesActivitySummarizer(),
                    "MovesDataStream").setTargetStream(activitySummaryStreamForMoves)
                    .setTimeWindowSize(Days.ONE);

            builder.setTask("TimeLeaveReturnHome", new MovesTimeLeaveReturnHome(),
                    "MovesDataStream").setTargetStream(leaveReturnHomeStreamForMoves)
                    .setTimeWindowSize(Days.ONE);
        }
        builder.submitToLocalCluster(topologyName);

        // sleep forever until interrupted
        while (true) {
            try {
                Thread.sleep(100000000);
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    public MobilityMovesTopology(String topologyName, int mobilitySpoutNum, int mobilityTaskParallelism) {
        this.topologyName = topologyName;
        this.mobility_spout_number = mobilitySpoutNum;
        this.parallelismPerTask = mobilityTaskParallelism;
    }
}
