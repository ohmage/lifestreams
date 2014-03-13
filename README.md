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

![alt text](http://storm.incubator.apache.org/documentation/images/topology.png "Storm topology")

To achieve further parallelsm, one can create multiple instances for the same spout or bolt. Storm provides several options to group the data and distribute the workload among these instances. For example, the following topology count the occurence of each word in randomly generated sentences.
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

Lifestreams is built on top of Storm and makes it extremely easy to implement a streaming pipeline for processing mHealth data for a large number of users. Lifestreams assumes that most of data should be grouped by the "user" (i.e. the owners of the data). A new premitive called ***task*** is provided. A task is similar to a bolt, but a new instance of it will be created for each invidual user. Consequently, it is guranteeed that:

> A task instance will only receive the data for one single user.

This allows the devloper to develop a multi-user data processing module as if there is only one single user. In addition, Lifestreams assumes that each data point is always associated with a timestamp, and provides built-in support for the tasks perform temporal data aggregatopn(e.g. the tasks compute *daily* activity summary or *weekly* sleep trend, etc). 

The recommended way to implement a task is to extend the SimpleTask class and implement the following methods
- executeDataPoint() will be called when receiving a new incoming data point in the current time window. Typicaly a task would update the computation state with the new data point in this method.
- finishWindow() will be called when all the data points in the current time window have been received and executed. A typical task would finalize the computation for the current time window, emit the result, and re-initialize the computation state.

For a concrete example, the following task computes the geo-diameter of a user (see here for full code).

``` java
public class SimpleGeoDiameter extends SimpleTask<MobilityData>{ // specify MobilityData as the input data type
    // a convex hull object
    ConvexHull curConvexHull = new ConvexHull();
    /**
     *  @params
     *  dp: the new incoming data point
     *  window: the current time window
     **/
	public void executeDataPoint(StreamRecord<MobilityData> dp, TimeWindow window) {
            // update the current convex hull with the new data point
			curConvexHull = updateCovexHull(dp, curCovexHull);
	}
    
    /** @params
     *  window: the current time window
     **/
	public void finishWindow(TimeWindow window) {
        // compute maximun geo-distance between any two vertexes
		computeMaximunGeoDistance(curConvexHull);
	    // clear convex hull
		curConvexHull.clear();
	}
}
``` 
You may wonder "what is a StreamRecord?"
### Seemless integration with ohmage
In addition, Lifestreams also make it dead simple to integrate with [ohmage], which is a data store for mHealth data. 

License
----

Apache 2.0

[storm]:http://storm.incubator.apache.org/
[ohmage]:http://ohmage.org/
    
