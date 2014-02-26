package lifestreams;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.Resource;

import lifestreams.bolt.ActivitySummaryBolt;
import lifestreams.bolt.GeoDistanceBolt;
import lifestreams.bolt.MobilityEventSmoothingBolt;
import lifestreams.model.MobilityDataPoint;

import org.ohmage.models.OhmageStream;
import org.ohmage.models.OhmageUser;
import org.ohmage.models.OhmageUser.OhmageAuthenticationError;

import lifestreams.spout.OhmageStreamSpout;

import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.base.BaseSingleFieldPeriod;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.SpringApplicationContextLoader;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import redis.clients.jedis.Jedis;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.fasterxml.jackson.core.JsonParseException;

import de.javakaffee.kryoserializers.jodatime.JodaDateTimeSerializer;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations={"classpath*:/mainContext.xml","classpath*:/users.xml"})
public class OhmageStreamTests {

	@Resource
    List<OhmageUser> users;
	@Autowired
    DateTime since;
	@Autowired
	OhmageStream mobilityStream;
	@Autowired
	OhmageStream activitySummaryStream;

	@Test
	public void testTopology() throws InterruptedException, JsonParseException, IOException {
		Jedis jedis = new Jedis("localhost");
		jedis.flushDB();
		 TopologyBuilder builder = new TopologyBuilder();
		 OhmageStreamSpout<MobilityDataPoint> mobilitySpout = new OhmageStreamSpout<MobilityDataPoint>(mobilityStream, users, since, MobilityDataPoint.class);
			
		 builder.setSpout("MobilityDataStream", mobilitySpout , 1);
		 BaseSingleFieldPeriod period = Days.ONE;
		 
		 // setup the topology
		 builder.setBolt("GeoDistanceBolt", new GeoDistanceBolt(period), 1).fieldsGrouping("MobilityDataStream", new Fields("user"));
		 
		 builder.setBolt("HMMMobilityStateRectifier", new MobilityEventSmoothingBolt(period), 1)
		 	.fieldsGrouping("MobilityDataStream", new Fields("user"));
		 
		 builder.setBolt("MobilityActivitySummarier", new ActivitySummaryBolt(period, activitySummaryStream), 1) //
		 	.fieldsGrouping("HMMMobilityStateRectifier",new Fields("user"));
		 
		 Config conf = new Config();
	     conf.setDebug(false);
	     conf.setMaxTaskParallelism(3);
	     conf.registerSerialization( DateTime.class, JodaDateTimeSerializer.class );

	     LocalCluster cluster = new LocalCluster();
	     cluster.submitTopology("Lifestreams-on-storm", conf, builder.createTopology());
	     while(true)
	    	 Thread.sleep(100000000);

	     //cluster.shutdown();

	}


}
