package lifestreams;

import java.io.IOException;
import java.util.List;

import javax.annotation.Resource;

import lifestreams.bolts.BasicLifestreamsBolt;
import lifestreams.models.data.MobilityData;

import org.ohmage.models.OhmageStream;
import org.ohmage.models.OhmageUser;

import lifestreams.spouts.OhmageStreamSpout;
import lifestreams.tasks.GeoDiameterTask;
import lifestreams.tasks.mobility.HMMMobilityRectifier;
import lifestreams.tasks.mobility.MobilityActivitySummarizer;
import lifestreams.tasks.mobility.PlaceDetection;
import lifestreams.tasks.moves.ActivitySummarizer;
import lifestreams.tasks.moves.TrackPointExtractor;
import lifestreams.utils.KryoSerializer;
import lifestreams.utils.SimpleTopologyBuilder;

import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.Seconds;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import redis.clients.jedis.Jedis;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import co.nutrino.api.moves.impl.dto.storyline.MovesSegment;

import com.fasterxml.jackson.core.JsonParseException;

import de.javakaffee.kryoserializers.jodatime.JodaDateTimeSerializer;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath*:/mainContext.xml",
		"classpath*:/users.xml" })
public class OhmageStreamTests {

	@Resource
	List<OhmageUser> users;
	@Autowired
	DateTime since;
	@Autowired
	OhmageStream mobilityStream;
	@Autowired
	OhmageStream activitySummaryStream;
	@Autowired
	OhmageStream movesSegmentStream;
	@Autowired
	OhmageStream geodiameterStream;
	@Autowired
	OhmageStream leaveArriveHomeStream;
	@Test
	public void testTopology() throws InterruptedException, JsonParseException,
			IOException {

		
		OhmageStreamSpout<MobilityData> mobilitySpout = new OhmageStreamSpout<MobilityData>(
				mobilityStream, users, since, MobilityData.class);
		OhmageStreamSpout<MovesSegment> movesSpout = new OhmageStreamSpout<MovesSegment>(
				movesSegmentStream, users, since, MovesSegment.class);

		/** setup the topology **/
		SimpleTopologyBuilder builder = new SimpleTopologyBuilder();
		// set the number of parallelism per task as the number of users
		int parallelismPerTask = users.size();
		
		/** Topology part 1. create a spout that gets mobility data and the tasks that consume the data **/
	
		builder.setSpout("MobilityDataStream", mobilitySpout);
		
		builder.setTask("PlaceDetection", new PlaceDetection(), "MobilityDataStream")
					.setParallelismHint(2)
					.setTimeWindowSize(Days.ONE)
					.setTargetStream(leaveArriveHomeStream);
		
		// compute daily geodiameter from Mobility data
		builder.setTask("GeoDistanceBolt", new GeoDiameterTask(), "MobilityDataStream")
					.setTargetStream(geodiameterStream)
					.setParallelismHint(2)
					.setTimeWindowSize(Days.ONE);
					
		// HMM model to correct shor-term errors in Mobility
		builder.setTask("HMMMobilityStateRectifier", new HMMMobilityRectifier(), "MobilityDataStream")
					.setParallelismHint(2)
					.setTimeWindowSize(Days.ONE);
		// based on the corrected Mobility data, compute daily activity summary
		builder.setTask("MobilityActivitySummarier", new MobilityActivitySummarizer(), "HMMMobilityStateRectifier")
					.setTargetStream(activitySummaryStream)
					.setTimeWindowSize(Days.ONE);


		/** Topology part 2. create a spout that gets Moves data and the tasks that consume the data **/
		
		builder.setSpout("MovesDataStream", movesSpout);

		// extract track points from moves segments
		builder.setTask("MovesTrackPointExtractor", new TrackPointExtractor(), "MovesDataStream")
				.setTimeWindowSize(Seconds.ONE);
		
		// compute geo diameter based on the track points
		builder.setTask("MovesGeoDistance", new GeoDiameterTask(), "MovesTrackPointExtractor")
				.setTargetStream(geodiameterStream)
				.setTimeWindowSize(Days.ONE);
		
		// generate daily activity summary
		builder.setTask("MovesActivitySummarier", new ActivitySummarizer(), "MovesDataStream")
				.setTimeWindowSize(Days.ONE);
		
		Config conf = new Config();
		conf.setDebug(false);
		
		// if it is a dryrun? if so, no data will be writeback to ohmage
		conf.put(LifestreamsConfig.DRYRUN_WITHOUT_UPLOADING, false);
		// keep the computation states in a local database or not.
		conf.put(LifestreamsConfig.ENABLE_STATEFUL_FUNCTION, false);
		
		// register all the classes used in Lifestreams framework to the kryo serializer
		KryoSerializer.setRegistrationsForStormConfig(conf);


		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("Lifestreams-on-storm", conf, builder.createTopology());
		
		while (true){
			Thread.sleep(100000000);
		}


	}

}
