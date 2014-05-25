package org.ohmage.lifestreams;

import org.joda.time.Days;
import org.ohmage.lifestreams.models.data.MobilityData;
import org.ohmage.lifestreams.spouts.MovesSpout;
import org.ohmage.lifestreams.spouts.OhmageStreamSpout;
import org.ohmage.lifestreams.tasks.GeoDiameterTask;
import org.ohmage.lifestreams.tasks.mobility.HMMMobilityRectifier;
import org.ohmage.lifestreams.tasks.mobility.MobilityActivitySummarizer;
import org.ohmage.lifestreams.tasks.mobility.PlaceDetection;
import org.ohmage.lifestreams.tasks.mobility.TimeLeaveReturnHome;
import org.ohmage.lifestreams.tasks.moves.FilterDuplicatedSegment;
import org.ohmage.lifestreams.tasks.moves.MovesActivitySummarizer;
import org.ohmage.lifestreams.tasks.moves.TrackPointExtractor;
import org.ohmage.models.OhmageStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import co.nutrino.api.moves.impl.service.MovesSecurityManager;

/**
 * This topology can process Mobility and Moves data and generate daily
 * summaries including: daily activity summaries, daily geodiameter, and time
 * leave/return home.
 * 
 * @author changun
 * 
 */
@Component
public class MobilityMovesTopology {
	// ** Input Streams **//
	@Autowired
	OhmageStream mobilityStream;
	@Autowired
	OhmageStream movesSegmentStream;
	@Autowired
	OhmageStream movesCredentialStream;

	// ** Output streams **//
	@Autowired
	OhmageStream activitySummaryStream;
	@Autowired
	OhmageStream geodiameterStream;
	@Autowired
	OhmageStream leaveArriveHomeStream;

	@Autowired
	MovesSecurityManager movesSecurityManger;

	// ** Spouts ** //
	@Autowired
	OhmageStreamSpout<MobilityData> mobilitySpout;
	@Autowired
	MovesSpout movesSpout;

	// ** Mobility components ** //
	@Autowired
	TimeLeaveReturnHome timeLeaveReturnHome;
	@Autowired
	GeoDiameterTask geoDiameterTask;
	@Autowired
	HMMMobilityRectifier HMMMobilityRectifier;
	@Autowired
	MobilityActivitySummarizer mobilityActivitySummarizer;
	@Autowired
	PlaceDetection placeDetection;
	// ** Moves components ** //
	@Autowired
	FilterDuplicatedSegment filterDuplicatedSegment;
	@Autowired
	TrackPointExtractor trackPointExtractor;
	@Autowired
	org.ohmage.lifestreams.tasks.moves.MovesTimeLeaveReturnHome movesTimeLEaveReturnHome;
	@Autowired
	MovesActivitySummarizer movesActivitySummarizer;

	// ** Configuration ** //
	@Value("${topology.name}")
	String topologyName;

	@Value("${parallelism.per.mobility.task}")
	int parallelismPerTask;

	@Value("${mobility.spout.number}")
	int mobility_spout_number;

	@Value("${enable.mobility.topology}")
	boolean enableMobility;

	@Value("${enable.moves.topology}")
	boolean enableMoves;

	@Autowired
	LifestreamsTopologyBuilder builder;

	public void run() {
		/** setup the topology **/

		/**
		 * Topology part 1. create a spout that gets mobility data and the tasks
		 * that consume the data
		 **/
		if (enableMobility) {
			builder.setSpout("MobilityDataStream", mobilitySpout,
					mobility_spout_number);

			builder.setTask("PlaceDetection", placeDetection, "MobilityDataStream")
					.setParallelismHint(parallelismPerTask);

			builder.setTask("MobilityTimeLeaveReturnHome", timeLeaveReturnHome, "PlaceDetection")
			.setParallelismHint(parallelismPerTask)
			.setTargetStream(leaveArriveHomeStream);
			
			// compute daily geodiameter from Mobility data
			builder.setTask("GeoDistanceBolt", geoDiameterTask,	"MobilityDataStream")
					.setTargetStream(geodiameterStream)
					.setParallelismHint(parallelismPerTask)
					.setTimeWindowSize(Days.ONE);

			// HMM model to correct shor-term errors in Mobility
			builder.setTask("HMMMobilityStateRectifier", HMMMobilityRectifier, "MobilityDataStream")
					.setParallelismHint(parallelismPerTask);
			// based on the corrected Mobility data, compute daily activity
			// summary
			builder.setTask("MobilityActivitySummarier",
					mobilityActivitySummarizer, "HMMMobilityStateRectifier")
					.setParallelismHint(parallelismPerTask)
					.setTargetStream(activitySummaryStream)
					.setTimeWindowSize(Days.ONE);
		}

		/**
		 * Topology part 2. create a spout that gets Moves data and the tasks
		 * that consume the data
		 **/
		if (enableMoves) {
			builder.setSpout("RawMovesDataStream", movesSpout, 1);

			// segments from the ohmage or the local Moves fetcher may contain
			// duplication. Filter them out.
			builder.setTask("MovesDataStream", filterDuplicatedSegment,	"RawMovesDataStream")
					.setTargetStream(movesSegmentStream);

			// extract track points from moves segments
			builder.setTask("MovesTrackPointExtractor", trackPointExtractor,
					"MovesDataStream");

			// compute geo diameter based on the track points
			builder.setTask("MovesGeoDiameter", geoDiameterTask,
					"MovesTrackPointExtractor")
					.setTargetStream(geodiameterStream)
					.setTimeWindowSize(Days.ONE);

			// generate daily activity summary
			builder.setTask("MovesActivitySummarier", movesActivitySummarizer,
					"MovesDataStream").setTargetStream(activitySummaryStream)
					.setTimeWindowSize(Days.ONE);

			builder.setTask("TimeLeaveReturnHome", movesTimeLEaveReturnHome,
					"MovesDataStream").setTargetStream(leaveArriveHomeStream)
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
}
