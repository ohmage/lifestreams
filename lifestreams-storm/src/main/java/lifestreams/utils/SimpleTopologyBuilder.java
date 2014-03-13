package lifestreams.utils;

import java.util.ArrayList;
import java.util.List;

import lifestreams.bolts.BasicLifestreamsBolt;
import lifestreams.tasks.SimpleTask;

import org.joda.time.Days;
import org.joda.time.base.BaseSingleFieldPeriod;
import org.ohmage.models.OhmageStream;

import backtype.storm.generated.StormTopology;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.SpoutDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class SimpleTopologyBuilder {
	TopologyBuilder builder = new TopologyBuilder();
	List<BoltConfig> boltConfigs = new ArrayList<BoltConfig>();
	
	public class BoltConfig{
		final String id;
		final String source;
		final SimpleTask task;
		int  parallelism_hint = 1;
		OhmageStream targetStream = null;
		BaseSingleFieldPeriod windowSize = Days.ONE;
		
		BoltConfig(String id, SimpleTask task, String source){
			this.id = id;
			this.task = task;
			this.source = source;
		}
		public BoltConfig setTargetStream(OhmageStream stream){
			this.targetStream = stream;
			return this;
		}
		public BoltConfig setTimeWindowSize(BaseSingleFieldPeriod windowSize){
			this.windowSize = windowSize;
			return this;
		}
		public BoltConfig setParallelismHint(int hint){
			this.parallelism_hint = hint;
			return this;
		}
		private void buildBolt(){
			BasicLifestreamsBolt bolt = new BasicLifestreamsBolt(task, windowSize);
			bolt.setTargetStream(targetStream);
			BoltDeclarer declarer = builder.setBolt(id, bolt, parallelism_hint)
					.fieldsGrouping(source, new Fields("user"));
		}
	}

	
	public BoltConfig setTask(String id, SimpleTask task, String source){
		BoltConfig config = new BoltConfig(id, task, source);
		this.boltConfigs.add(config);
		// return a BoltConfig object for further configuration
		return config;
	}

	public void setSpout(String id, IRichSpout spout) {
		// add the spout into the topology
		SpoutDeclarer declarer = builder.setSpout(id, spout);
		
	}
	public StormTopology createTopology(){
		for(BoltConfig config: boltConfigs){
			config.buildBolt();
		}
		return builder.createTopology();
	}
}
