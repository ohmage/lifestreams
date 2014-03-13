Lifestreams
=========

Lifestreams is a streaming processing pipeline for processing mHealth data for a large number of users. The main features are:
  - Seemless concurrent processing for multiple users' data
  - Dead simple integration with [ohmage]
  - Built-in support for temporal data aggreation
  - Fail-safe stateful computation


Lifestreams Arcchitecture
====
Lifestreams is based on [Storm], a distributed streaming process framework that makes it easy to create, deploy, and scale a streaming computing topology. In this section, we will first go over several concepts in Storm, and then discuss what Lifestreams provides on top of it.

The basic primitives in Storm are **spouts** and **bolts**. A spout is a source of streams. e.g. a spout may read data from ohmage API and emit a stream of data points into the topology. A bolt consumes input streams, does some processing, and possibly emits new streams into the downstreaming bolts. 

![alt text](http://storm.incubator.apache.org/documentation/images/topology.png "A Storm topology")

To achieve further parallelism, one can create multiple instances for the same spout or bolt. Storm provides several options to group the data and distribute the workload among these instances. For example, the following topology count the occurence of each word in randomly generated sentences.
``` java
TopologyBuilder builder = new TopologyBuilder();
builder.setSpout("spout", new RandomSentenceSpout());
builder.setBolt("split", new SplitSentence(), 8).shuffleGrouping("spout");
builder.setBolt("count", new WordCount(), 12).fieldsGrouping("split", new Fields("word"));
```
(see [here](https://github.com/nathanmarz/storm-starter/blob/master/src/jvm/storm/starter/WordCountTopology.java) for full code)

As you may tell, *setSpout()* and *setBolt()* create a spout or a bolt. The first argument is the *id* of the node; the second argument is an instance of it; and the third (optional) argument is the parallelsm hint that storm will use to decide how many instances it should create for each node. 

Every bolt has to additionaly specify its stream source and the way the workload should be distributed. For example:
- In Line 3, SplitSentence bolt specifies its stream source as RandomSentenceSpout and the workload should be randomly shuffled among the 8 instances of it. 
- In Line 4, WordCount bolt specifies its source as the SplitSentence bolt and the workload should be ditributted by the *word* field of a data point. The storm gurantees that the data points of the same word will always go to the same instance of the WordCount bolt, so the words can be counted reliably.

So what Lifestreams provides on top of Storm?
------

Lifestreams, built on top of Storm, is aimed to make it extremely easy to implement a mHealth streaming pipeline for a large number of users. Lifestreams assumes that most of data should be grouped by the "user" (i.e. the owners of the data). A new premitive called **Task** is introduced. A task is similar to a bolt, but Lifestreams will create a new instance of a task for each invidual user. In other words, Lifestreams guranteees that:

> A user's data will always be distributed to the same instance of a task; and a task instance will only receive the data for the same user.

Such a process model allows the devloper to develop a multi-user data processing pipeline as if there is only one user. In addition, Lifestreams assumes that each data point is always associated with a timestamp, and provides built-in support for the tasks that perform temporal data aggregatoin(e.g. the tasks compute *daily* activity summary or *weekly* sleep trend, etc). 

The recommended way to implement a Task is to extend the SimpleTask class and implement the following methods
- *executeDataPoint()* will be called when receiving a new incoming data point in the current time window. Typicaly a task would update the computation state with the new data point in this method.
- *finishWindow()* will be called when all the data points in the current time window have been received and executed. A typical task would finalize the computation for the current time window, emit the result, and re-initialize the computation state.

For a concrete example, the following task computes the geo-diameter of a user (see here for full code).

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
		ActivityInstanceCountData output = new ActivityInstanceCountData(activeInstanceCount);
        // create a stream record with the output data as payload
        createRecord()
            .setTimestamp(window.getBeginOfTimeWindow())
            .setData(output)
            .emitRecord(); // emit the record to the downstreaming nodes
	    // clear the counter
		activeInstanceCount = 0;
	}
}
``` 
You may wonder "what on earth is a **StreamRecord**?" A StreamRecord is the container object used in Lifestreams to transfer data between spouts and tasks. A StreamRecord is associated with a user and a timestamp, and optionally with a geo-location. The payload of a stream record is accessible through **getData()/setData()** methods (or **d()/d(data)** for short). For example:

```  java
// a stream record that contains a mobility data point
StreamRecord<MobilityData> rec; 
/* get data */
MobilityData data = rec.getData();     // or MobilityData data = rec.d(); 
/* set data */
rec.setData(data);  // or rec.d(data); 
``` 
The payload of a stream record can be any serializable Java object. However, it is strongly recomended to define a POJO class for any data type you generate in Lifestreams, so that the other modules can have a defined "data contract" they can be built upon. (see [MobilityData.java](https://github.com/ohmage/lifestreams/blob/master/lifestreams-storm/src/main/java/lifestreams/models/data/MobilityData.java) for example, or [more](https://github.com/ohmage/lifestreams/tree/master/lifestreams-storm/src/main/java/lifestreams/models/data).) 

As shown in the above example, one can create and emit a stream record by using the createRecord() method provided by SimpleTask class.

### Seemless integration with ohmage
In addition, Lifestreams also makes it dead simple to integrate with [ohmage], which is a data store for mHealth data. For data query, one can use OhmageStreamSpout to query that data and listen to the updates from Ohmage Stream API. For example, the following code creates a spout that query the Mobility data stream for 2 users since 2013-1-1.

``` java
DateTime since = new DateTime("2013-1-1");
OhmageStream stream = new OhmageStream.Builder()
			.observerId("edu.ucla.cens.Mobility")
			.observerVer("2012061300")
			.streamId("regular")
			.streamVer("2012050700").build();
// A list of users we are querying for
List<OhmageUser> users = new ArrayList<OhmageUser>()
users.add(new OhmageUser("https://test.ohmage.org", "LifestreamsTest1", "password"));
users.add(new OhmageUser("https://test.ohmage.org", "LifestreamsTest2", "password"));
// create a spout that query the data since 2013-1-1 and keep polling for updates of data
OhmageStreamSpout<MobilityData> mobilitySpout = new OhmageStreamSpout<MobilityData>(mobilityStream, users, since, MobilityData.class);
``` 
For data uploading, one can specify the target stream to upload to for any arbitrary Tasks. Then, when a task outputs a stream record, it will be automatically uploaded to ohmage!

/** setup the topology **/
SimpleTopologyBuilder builder = new SimpleTopologyBuilder();
// set the number of parallelism per task as the number of users
int parallelismPerTask = users.size();

/** Topology part 1. create a spout that gets mobility data and the tasks that consume the data **/

builder.setSpout("MobilityDataStream", mobilitySpout);

builder.setTask("PlaceDetection", new TimeLeaveReturnHome(), "MobilityDataStream")
			.setParallelismHint(2)
			.setTimeWindowSize(Days.ONE)
			.setTargetStream(leaveArriveHomeStream);
``` 
License
----

Apache 2.0

[storm]:http://storm.incubator.apache.org/
[ohmage]:http://ohmage.org/

[john gruber]:http://daringfireball.net/
[@thomasfuchs]:http://twitter.com/thomasfuchs
[1]:http://daringfireball.net/projects/markdown/
[marked]:https://github.com/chjj/marked
[Ace Editor]:http://ace.ajax.org
[node.js]:http://nodejs.org
[Twitter Bootstrap]:http://twitter.github.com/bootstrap/
[keymaster.js]:https://github.com/madrobby/keymaster
[jQuery]:http://jquery.com
[@tjholowaychuk]:http://twitter.com/tjholowaychuk
[express]:http://expressjs.com

    
