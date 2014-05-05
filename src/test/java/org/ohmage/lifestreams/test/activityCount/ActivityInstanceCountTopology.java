package org.ohmage.lifestreams.test.activityCount;

import org.joda.time.DateTime;
import org.joda.time.Days;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ohmage.lifestreams.LifestreamsConfig;
import org.ohmage.lifestreams.spouts.OhmageStreamSpout;
import org.ohmage.lifestreams.utils.KryoSerializer;
import org.ohmage.lifestreams.utils.SimpleTopologyBuilder;
import org.ohmage.models.OhmageStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import backtype.storm.Config;
import backtype.storm.LocalCluster;



@ContextConfiguration({"classpath*:/users.xml", "classpath*:/mainContext.xml", "classpath:/testContext.xml"})
@RunWith(SpringJUnit4ClassRunner.class)
public class ActivityInstanceCountTopology {
	@Autowired // output stream
	OhmageStream activityInstanceCountStream;

	@Autowired // spout that emits mobility data
	OhmageStreamSpout mobilitySpout;
	
	@Autowired // activityInstanceCounter
	ActivityInstanceCounter activityInstanceCounter;
	
	@Test
	public void run() throws InterruptedException{
		// since when to perform the computation
		DateTime since = new DateTime("2013-1-1");
		/** setup the input and output streams **/

		/** setup the topology **/
		SimpleTopologyBuilder builder = new SimpleTopologyBuilder();
		
		// set the number of parallelism for each task to be 5
		int parallelismPerTask = 5;
		
		builder.setSpout("MobilitySpout", mobilitySpout);
		
		// create a ActivityInstanceCounter task, with MobilitySpout as input source
		builder.setTask("ActivityInstanceCount", activityInstanceCounter, "MobilitySpout")
					.setParallelismHint(parallelismPerTask) // num of parallelism = num of users
					.setTimeWindowSize(Days.ONE) // aggregate the data by days
					.setTargetStream(activityInstanceCountStream); //output data to ohmage
		

		Config conf = new Config();
		conf.setDebug(false);
		
		// if it is a dryrun? if so, no data will be writeback to ohmage
		conf.put(LifestreamsConfig.DRYRUN_WITHOUT_UPLOADING, true);
		// keep the computation states in a local database or not.
		conf.put(LifestreamsConfig.ENABLE_STATEFUL_FUNCTION, false);
		
		// register all the classes used in Lifestreams framework to the kryo serializer
		KryoSerializer.setRegistrationsForStormConfig(conf);
	
		LocalCluster cluster = new LocalCluster();
		// run the cluster locally
		cluster.submitTopology("Lifestreams-on-storm", conf, builder.createTopology());
		

		Thread.sleep(1000 * 60 * 1);

	}

	
}
