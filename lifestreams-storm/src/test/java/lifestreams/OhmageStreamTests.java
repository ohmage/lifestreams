package lifestreams;

import java.io.IOException;
import java.util.List;

import javax.annotation.Resource;

import lifestreams.bolt.GeoDiameterTask;
import lifestreams.bolt.SimpleLifestreamsBolt;
import lifestreams.bolt.mobility.ActivitySummaryBolt;
import lifestreams.bolt.mobility.MobilityEventSmoothingBolt;
import lifestreams.bolt.moves.ActivitySummarizer;
import lifestreams.bolt.moves.TrackPointExtractor;
import lifestreams.model.data.MobilityData;

import org.ohmage.models.OhmageStream;
import org.ohmage.models.OhmageUser;
import lifestreams.spout.OhmageStreamSpout;

import org.joda.time.DateTime;
import org.joda.time.Days;
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

	@Test
	public void testTopology() throws InterruptedException, JsonParseException,
			IOException {
		Jedis jedis = new Jedis("localhost");
		jedis.flushDB();
		TopologyBuilder builder = new TopologyBuilder();
		OhmageStreamSpout<MobilityData> mobilitySpout = new OhmageStreamSpout<MobilityData>(
				mobilityStream, users, since, MobilityData.class);
		OhmageStreamSpout<MovesSegment> movesSpout = new OhmageStreamSpout<MovesSegment>(
				movesSegmentStream, users, since, MovesSegment.class);

		builder.setSpout("MobilityDataStream", mobilitySpout, 1);
		// setup the topology
		// builder.setBolt("GeoDistanceBolt", new GeoDistanceBolt(Days.ONE),
		// 1).fieldsGrouping("MobilityDataStream", new Fields("user"));

		builder.setBolt(
				"GeoDistanceBolt",
				new SimpleLifestreamsBolt(new GeoDiameterTask(), Days.ONE,
						geodiameterStream), 1).fieldsGrouping(
				"MobilityDataStream", new Fields("user"));

		builder.setBolt("HMMMobilityStateRectifier",
				new MobilityEventSmoothingBolt(Days.ONE), 1).fieldsGrouping(
				"MobilityDataStream", new Fields("user"));

		builder.setBolt("MobilityActivitySummarier",
				new ActivitySummaryBolt(Days.ONE, activitySummaryStream), 1)
				//
				.fieldsGrouping("HMMMobilityStateRectifier", new Fields("user"));

		/* The following is the bolt for Moves */
		builder.setSpout("MovesDataStream", movesSpout, 1);

		// extract track points from moves segments
		builder.setBolt("MovesTrackPointExtractor",
				new SimpleLifestreamsBolt(new TrackPointExtractor(), Days.ONE),
				1).fieldsGrouping("MovesDataStream", new Fields("user"));
		// compute geo diameter based on the track points
		builder.setBolt("MovesGeoDistance",
				new SimpleLifestreamsBolt(new GeoDiameterTask(), Days.ONE), 1)
				.fieldsGrouping("MovesTrackPointExtractor", new Fields("user"));
		// generate daily activity summary
		builder.setBolt(
				"MovesActivitySummarier",
				new SimpleLifestreamsBolt(new ActivitySummarizer(), Days.ONE,
						activitySummaryStream), 1) //
				.fieldsGrouping("MovesDataStream", new Fields("user"));

		Config conf = new Config();
		conf.setDebug(false);
		conf.setMaxTaskParallelism(3);
		conf.registerSerialization(DateTime.class, JodaDateTimeSerializer.class);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("Lifestreams-on-storm", conf,
				builder.createTopology());
		while (true)
			Thread.sleep(100000000);

		// cluster.shutdown();

	}

}
