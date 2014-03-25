package org.ohmage.lifestreams.examples.activityCount;

import java.util.ArrayList;
import java.util.List;

import org.joda.time.DateTime;
import org.joda.time.Days;
import org.ohmage.lifestreams.LifestreamsConfig;
import org.ohmage.lifestreams.models.data.MobilityData;
import org.ohmage.lifestreams.spouts.OhmageStreamSpout;
import org.ohmage.lifestreams.utils.KryoSerializer;
import org.ohmage.lifestreams.utils.SimpleTopologyBuilder;
import org.ohmage.models.OhmageStream;
import org.ohmage.models.OhmageUser;

import backtype.storm.Config;
import backtype.storm.LocalCluster;

public class ActivityInstanceCountTopology {

	public static void main(String [ ] args) throws InterruptedException{
		// since when to perform the computation
		DateTime since = new DateTime("2013-1-1");
		/** setup the input and output streams **/
		// the input mobility stream
		OhmageStream mobilityStream = new OhmageStream.Builder()
									.observerId("edu.ucla.cens.Mobility")
									.observerVer("2012061300")
									.streamId("extended")
									.streamVer("2012050700").build();
		// the output stream
		OhmageStream activityInstanceCountStream = new OhmageStream.Builder()
									.observerId("org.ohmage.lifestreams.example")
									.observerVer("201403111")
									.streamId("activity_instance_count")
									.streamVer("20140311").build();
		
		//A list of users we are processing the data for
		List<OhmageUser> users = new ArrayList<OhmageUser>();
		users.add(new OhmageUser("https://test.ohmage.org", "LifestreamsTest1", "FILL_IN_THE PASSWORD"));
		users.add(new OhmageUser("https://test.ohmage.org", "LifestreamsTest2", "FILL_IN_THE PASSWORD"));

		/** setup the topology **/
		SimpleTopologyBuilder builder = new SimpleTopologyBuilder();
		
		// set the number of parallelism per task as the number of users
		int parallelismPerTask = users.size();
		
		// spout that queries mobility data
				OhmageStreamSpout<MobilityData> mobilitySpout = 
						new OhmageStreamSpout<MobilityData>(mobilityStream, users, since, MobilityData.class);
				
		builder.setSpout("MobilitySpout", mobilitySpout);
		
		// create a ActivityInstanceCounter task, with MobilitySpout as input source
		builder.setTask("ActivityInstanceCount", new ActivityInstanceCounter(), "MobilitySpout")
					.setParallelismHint(parallelismPerTask) // num of parallelism = num of users
					.setTimeWindowSize(Days.ONE) // aggregate the data by days
					.setTargetStream(activityInstanceCountStream); //output data to ohmage
		

		Config conf = new Config();
		conf.setDebug(false);
		
		// if it is a dryrun? if so, no data will be writeback to ohmage
		conf.put(LifestreamsConfig.DRYRUN_WITHOUT_UPLOADING, false);
		// keep the computation states in a local database or not.
		conf.put(LifestreamsConfig.ENABLE_STATEFUL_FUNCTION, false);
		
		// register all the classes used in Lifestreams framework to the kryo serializer
		KryoSerializer.setRegistrationsForStormConfig(conf);
	
		LocalCluster cluster = new LocalCluster();
		// run the cluster locally
		cluster.submitTopology("Lifestreams-on-storm", conf, builder.createTopology());
		
		// sleep forever...
		while (true){
			Thread.sleep(100000000);
		}
	}
}
