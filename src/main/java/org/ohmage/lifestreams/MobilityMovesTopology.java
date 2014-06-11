package org.ohmage.lifestreams;

import org.joda.time.Days;
import org.joda.time.Duration;
import org.ohmage.lifestreams.models.data.MobilityData;
import org.ohmage.lifestreams.spouts.MovesSpout;
import org.ohmage.lifestreams.spouts.OhmageStreamSpout;
import org.ohmage.lifestreams.tasks.DataRateLimiter;
import org.ohmage.lifestreams.tasks.GeoDiameterTask;
import org.ohmage.lifestreams.tasks.mobility.HMMMobilityRectifier;
import org.ohmage.lifestreams.tasks.mobility.MobilityActivitySummarizer;
import org.ohmage.lifestreams.tasks.mobility.PlaceDetection;
import org.ohmage.lifestreams.tasks.mobility.TimeLeaveReturnHome;
import org.ohmage.lifestreams.tasks.moves.FilterDuplicatedSegment;
import org.ohmage.lifestreams.tasks.moves.MovesActivitySummarizer;
import org.ohmage.lifestreams.tasks.moves.MovesTimeLeaveReturnHome;
import org.ohmage.lifestreams.tasks.moves.TrackPointExtractor;
import org.ohmage.models.OhmageStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

/**
 * This topology processes Mobility and Moves data and generate daily
 * summaries including: daily activity summaries, daily geodiameter, and time
 * leave/return home.
 * 
 * @author changun
 * 
 */
class MobilityMovesTopology {

	// ** Mobility Output streams **//
	@Autowired
	@Qualifier("activitySummaryStream")
    private
    OhmageStream activitySummaryStream;
	@Autowired
	@Qualifier("geodiameterStream")
    private
    OhmageStream geodiameterStream;
	@Autowired
	@Qualifier("leaveReturnHomeStream")
    private
    OhmageStream leaveReturnHomeStream;

    // ** Moves Output streams **//
    @Autowired
    @Qualifier("activitySummaryStreamForMoves")
    private
    OhmageStream activitySummaryStreamForMoves;
    @Autowired
    @Qualifier("geodiameterStreamForMoves")
    private
    OhmageStream geodiameterStreamForMoves;
    @Autowired
    @Qualifier("leaveReturnHomeStreamForMoves")
    private
    OhmageStream leaveReturnHomeStreamForMoves;

	// ** Spouts ** //
	@Autowired
    private
    OhmageStreamSpout<MobilityData> mobilitySpout;
	@Autowired
    private
    MovesSpout movesSpout;
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

			builder.setTask("PlaceDetection", new PlaceDetection(), "ThrottledMobilityDataStream")
					.setParallelismHint(parallelismPerTask);

			builder.setTask("MobilityTimeLeaveReturnHome", new TimeLeaveReturnHome(), "PlaceDetection")
			.setParallelismHint(parallelismPerTask)
			.setTargetStream(leaveReturnHomeStream);
			
			// compute daily geodiameter from Mobility data
			builder.setTask("GeoDistanceBolt", new GeoDiameterTask(),	"ThrottledMobilityDataStream")
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

			builder.setSpout("RawMovesDataStream", movesSpout, 1);

			// segments from the ohmage or the local Moves fetcher may contain
			// duplication. Filter them out.
			builder.setTask("MovesDataStream", new FilterDuplicatedSegment(),	"RawMovesDataStream");

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
	 public MobilityMovesTopology(String topologyName, int mobilitySpoutNum, int mobilityTaskParallelism){
		 this.topologyName = topologyName;
		 this.mobility_spout_number = mobilitySpoutNum;
		 this.parallelismPerTask = mobilityTaskParallelism;
	 }
}
