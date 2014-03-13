(**before you start, use the following command to install the openmap.jar to your local maven repo**: mvn install:install-file -Dfile=libs/openmap.jar -DgroupId=bnn -DartifactId=openmap -Dversion=5.0.3 -Dpackaging=jar)

Lifestreams
=========

Lifestreams is a streaming processing pipeline for processing mHealth data for a large number of users. The main features are:
  - Seamless concurrent processing for multiple users' data
  - Integration with [ohmage] like a breeze
  - Built-in support for temporal data aggregation
  - Fail-safe stateful computation

(For impetient readers: see [ActivityInstanceCount](https://github.com/ohmage/lifestreams/tree/master/lifestreams-storm/src/main/java/lifestreams/examples/activityCount) for a working example.)

Lifestreams Architecture
====
Lifestreams is based on [Storm], a distributed streaming process framework that makes it easy to create, deploy, and scale a streaming computing topology. In this section, we will first go over several concepts in Storm, and then discuss what Lifestreams provides on top of it.

The basic primitives in Storm are **spouts** and **bolts**. A spout is a source of streams. e.g. a spout may read data from ohmage stream API and emit a stream of data points into the topology. A bolt consumes input streams, does some processing, and possibly emits new streams into the downstreaming bolts. 

![alt text](http://storm.incubator.apache.org/documentation/images/topology.png "A Storm topology")

To achieve further parallelism, multiple instances for the same spout or bolt can be created. Storm provides several options to group the data and distribute the workload among these instances. See the following topology that counts the occurrence of words for example:
``` java
TopologyBuilder builder = new TopologyBuilder();
builder.setSpout("spout", new RandomSentenceSpout());
builder.setBolt("split", new SplitSentence(), 8).shuffleGrouping("spout");
builder.setBolt("count", new WordCount(), 12).fieldsGrouping("split", new Fields("word"));
```
(see [here](https://github.com/nathanmarz/storm-starter/blob/master/src/jvm/storm/starter/WordCountTopology.java) for full code)

As you may tell, *setSpout()* and *setBolt()* create a spout or a bolt. The first argument is the *id* of the node; the second argument is an instance of a spout or bolt; and the third (optional) argument is the parallelism hint that storm will use to determine how many instances it should create for each node to share the workload. 

In addition, each bolt has to specify its source and the way the workload should be divided among multiple instances of itself. For example:
- In Line 3, SplitSentence bolt specifies its source as the output of RandomSentenceSpout and the workload should be randomly shuffled among the 8 instances of it. 
- In Line 4, WordCount bolt specifies its source as the SplitSentence bolt and the workload should be divided by the *word* field of the data points. Storm guarantees that the data points of the same word will go to the same instance of the WordCount bolt, so that it can be counted reliably.

So what Lifestreams provides on top of Storm?
------

Lifestreams, built on top of Storm, is aimed to make it extremely easy to implement an mHealth data processing pipeline for a large number of users. Lifestreams assumes that the processing of an individual user's data can be separated from the the other users, and introduce a new primitive called **IndividualTask** (or **Task** for short). An IndividualTask is similar to a bolt with workload divided by the user, but with a stronger guarantee, that is:

> The same user's data will always go to to the same instance of an IndividualTask; and that IndividualTask instance will only receive the data for that particular user.

Such abstraction simplifies the mHealth module development by allowing the devlopers to develop  modules for processing multiple users' data as if there is only one single user. 

In addition, Lifestreams assumes that a timestamp is always associated with a data point, and provides built-in support for performing temporal aggregation tasks. Such tasks are ubiquitous in the mHealth area, examples including computing *daily* activity summary or analyzing the *weekly* sleep patterns, etc. Lifestreams maintain a **TimeWindow** for each IndividualTask instance, and will notify an instance when all the data in the current time window has been received.

The recommended way to implement an IndividualTask is to extend the SimpleTask class and implement the following methods.
- *executeDataPoint()* is called when receiving a new incoming data point in the current time window, so that the task can update the computation state with the new data point.
- *finishWindow()* is called when all the data points in the current time window have been received, so that a task can finalize the computation for the current time window, emit the result, and re-initialize the computation state.
- *snapshotWindow()* (***experimental***) is called when a front-end apps needs the computation results as soon as possible, even if we have not received all the data pin the current time yet. Typically, the snapshotWindow() method will perform the same computation as in finishWindow() method, but without reinitializing the computation state afterwards.

For a concrete example, the following task counts  a user's activity instances within each time window.

``` java
public class ActiveInstanceCounter extends SimpleTask<MobilityData>{// input data type = MobilityData
    int activeInstanceCount = 0; // the number of active instances
    /**
     *  @param dp: the new incoming data point
     *  @param window: the current time window
     **/
    public void executeDataPoint(StreamRecord<MobilityData> rec, TimeWindow window) {
            if(rec.getData().getMode().isActive()){
                // increment the counter if this is a active mobility instance
                activeInstanceCount ++;
            }
	}
    /** 
     *  @param window: the current time window
     **/
	public void finishWindow(TimeWindow window) {
        // create output data
		ActivityInstanceCountData output = new ActivityInstanceCountData(window, this);
        output.setActivityInstanceCount(activeInstanceCount);
        // create a stream record with the output data as payload
        createRecord()
            .setTimestamp(window.getFirstInstant())
            .setData(output)
            .emit(); // emit the record to the downstreaming nodes
	    // clear the counter
		activeInstanceCount = 0;
	}
    /** 
     *  @param window: the current time window
     **/
    public void snapshotWindow(TimeWindow window) {
        // create output data
		ActivityInstanceCountData output = new ActivityInstanceCountData(window, this);
        output.setActivityInstanceCount(activeInstanceCount);
        // create a stream record with the output data as payload
        createRecord()
            .setTimestamp(window.getFirstInstant())
            .setIsSnapshot(true)
            .setData(output)
            .emit(); // emit the record to the downstreaming nodes
	    // without clearing the counter
	}
}
``` 
You may wonder "what on earth is a **StreamRecord** in the code above?" A StreamRecord is a container object used in Lifestreams to transfer data among nodes spouts and tasks (as well as, integrating with ohmage, more in the next section). A StreamRecord contains metadata including 1) the owner of the record, 2) the timestamp, 3) and optionally the geo-location associated with that record. The payload of a stream record can be any serializable Java object, and is accessible through **getData()/setData()** methods (or **d()/d(data)** for short). 

For example:
```  java
// a stream record that contains a mobility data point
StreamRecord<MobilityData> rec; 
/* get data */
MobilityData data = rec.getData();     // or MobilityData data = rec.d(); 
/* set data */
rec.setData(data);  // or rec.d(data); 
``` 
While you can use a generic data type, such as HashMap, or ObjectNode for the payload, it is strongly recommended to define a POJO class for any data type you will generate, so that the future modules can have a defined "contract for data type" to depend on. (see [MobilityData.java](https://github.com/ohmage/lifestreams/blob/master/lifestreams-storm/src/main/java/lifestreams/models/data/MobilityData.java) for example, or [more](https://github.com/ohmage/lifestreams/tree/master/lifestreams-storm/src/main/java/lifestreams/models/data).) 

SimpleTask provides a helper function called createRecord(), that makes it easy to create and emit a StreamRecord.

### Seamless integration with ohmage
One important goal of Lifestreams is to make it a breeze to integrate with [ohmage], an open source data store used in many mHealth studies. For querying data from ohmage, OhmageStreamSpout is available to enable query and listening to the updates from Ohmage Stream API. For uploading data to ohmage, any Task can be associated with a *target stream*; consequently all the records output by the task will be automatically uploaded to ohmage! (Note that for a record to be successfully uploaded, the format of its data content must be compatible with the ohmage stream schema.)
 See the following code for example: (or see [here](https://github.com/ohmage/lifestreams/tree/master/lifestreams-storm/src/main/java/lifestreams/examples/activityCount) for a working example.)

``` java
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

		// set configurations
		Config conf = new Config();
		conf.setDebug(false);
		
		// if it is a dryrun? if so, no data will be writeback to ohmage
		conf.put(LifestreamsConfig.DRYRUN_WITHOUT_UPLOADING, false);
		// keep the computation states in a local database or not.
		conf.put(LifestreamsConfig.ENABLE_STATEFUL_FUNCTION, false);
		
		// register all the classes used in Lifestreams framework to the kryo serializer
		KryoSerializer.setRegistrationsForStormConfig(conf);
	
		LocalCluster cluster = new LocalCluster();
		// run the topology locally
		cluster.submitTopology("Lifestreams-on-storm", conf, builder.createTopology());
		
		// sleep forever...
		while (true){
			Thread.sleep(100000000);
		}
	}
}
``` 
License
----

Apache 2.0

[storm]:http://storm.incubator.apache.org/
[ohmage]:http://ohmage.org/


    

